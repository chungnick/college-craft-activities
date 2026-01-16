import json
import os
import sys
import argparse
import time
import csv
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
from google import genai
from google.genai import types
from dotenv import load_dotenv
from token_logger import log_tokens
from failed_tracker import log_failure, clear_failure

# Load environment variables
load_dotenv('.env.local')

# --- CONFIGURATION ---
INPUT_FILE = "ec_bank_rows_with_valid_url.csv"
MD_DIR = "md-files"
RESULTS_FILE = "results.json"
MODEL_NAME = "gemini-3-flash-preview"

# --- PROMPTS AND OPTIONS ---

def read_file_content(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

METADATA_OPTIONS = read_file_content("metadata.py")
TAGS_OPTIONS = read_file_content("options_tags.py")

PROMPT_DATES = """
You are a helpful assistant that extracts application deadlines and program dates from markdown content of a summer program page.

Extract the following information:
1. Application Deadlines: Any specific dates by which applications must be submitted.
2. Program Dates: The start and end dates of the program sessions.

Format the output as a valid JSON object with the following structure:
{
  "deadlines": [
    { "label": "Session 1", "dates": ["YYYY-MM-DD"] },
    { "label": "Financial Aid Deadline", "dates": ["YYYY-MM-DD"] }
  ],
  "deadlines_found": true, 
  "program_dates": [
    { "label": "Tech Crafters Session 1", "dates": ["YYYY-MM-DD", "YYYY-MM-DD"] }
  ],
  "program_dates_found": true
}

Rules:
- Dates should be in YYYY-MM-DD format.
- If a date range is given, include both start and end dates in the "dates" array.
- If a single date is given, the "dates" array will have one element.
- If no deadlines are found, set "deadlines_found" to false and "deadlines" to [].
- If no program dates are found, set "program_dates_found" to false and "program_dates" to [].
- "deadlines_found" and "program_dates_found" should be boolean values (true/false) in the JSON.
"""

PROMPT_METADATA = f"""
You are a data extraction assistant extracting structured metadata from a summer program's markdown page.

Use the following strict options for classification. If a field corresponds to one of these lists, you MUST choose values ONLY from the provided options.

{METADATA_OPTIONS}

Extract the following fields:
1. mode: (Choose from options provided)
2. price: (Choose from options provided - e.g. Free or Paid)
3. eligibility: (Choose from options provided)
4. grade_level: (Choose from options provided)
5. location: (Choose from options provided - e.g. State names or International)
6. program_type: (Choose from options provided)
7. financial_aid: (Yes/No based on options)
8. application_requirements: (Choose from options provided)

Format the output as a valid JSON object. 
If a field allows multi-select, return a list of strings. Otherwise, return a single string (or null).
"""

PROMPT_DESCRIPTION = f"""
You are an expert editor helping organize information about an extracurricular activity.
Given the RAW TEXT below, extract the Title, Subtitle, and Tags, and produce exactly ONE paragraph that is informational and easy to skim.

Use the following strict options for the 'tags' field. You MUST choose values ONLY from the provided options.

{TAGS_OPTIONS}

Requirements:
1. title: The name of the program being offered. This MUST be an exact match of what is presented in the text, not a summarization.
2. subtitle: The name of the institution that is offering the program. This MUST be an exact match of what is presented in the text, not a summarization.
3. tags: (Choose from the 'tags' options provided - Subject areas). Return as a list of strings.
4. description: Capture all important, activity-relevant details (who/what/logistics). Omit location and date/time. Do not invent facts. Keep it as a single cohesive paragraph, with a minimum of 4 sentences.

Format the output as a valid JSON object.
"""

# --- HELPERS ---

def get_combined_md_content(row_id: str) -> Optional[str]:
    # Use sources.json to determine which files are valid and avoid errors
    sources_path = os.path.join(MD_DIR, row_id, "sources.json")
    if not os.path.exists(sources_path):
        # Fallback to checking main0.md only if sources.json is missing
        main_path = os.path.join(MD_DIR, row_id, "main0.md")
        if not os.path.exists(main_path): return None
        with open(main_path, "r") as f: return f"--- MAIN PAGE ---\n{f.read()}\n\n"

    try:
        with open(sources_path, "r") as f:
            sources_data = json.load(f)
            # Support both old format (flat dict) and new format (dict with "files" key)
            valid_files = sources_data.get("files", sources_data)
    except:
        return None

    combined = ""
    # Process only files explicitly listed as successful in sources.json
    for filename in ["main0.md", "sister1.md", "sister2.md", "sister3.md"]:
        if filename in valid_files:
            path = os.path.join(MD_DIR, row_id, filename)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    label = "MAIN PAGE" if filename == "main0.md" else f"SISTER PAGE {filename[-4]}"
                    combined += f"--- {label} ---\n{f.read()}\n\n"
    
    return combined if combined else None

def call_gemini(client, prompt, content, step_name):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=f"{prompt}\n\nInput Markdown:\n{content}",
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            if response.usage_metadata:
                log_tokens(step_name, MODEL_NAME, response.usage_metadata.prompt_token_count, response.usage_metadata.candidates_token_count)
            return json.loads(response.text)
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "resource_exhausted" in err:
                time.sleep(60)
                continue
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            raise e
    return {}

def load_results():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                return data.get("results", [])
            except json.JSONDecodeError: return []
    return []

def save_results(results):
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump({"results": results}, f, indent=2)

# --- MAIN ---

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--repeat", action="store_true")
    parser.add_argument("--id", type=str, default=None, help="Process only this specific ID")
    args = parser.parse_args()

    # Initialize clients with separate keys
    key4 = os.environ.get("GEMINI_API_KEY_STEP_4")
    key5 = os.environ.get("GEMINI_API_KEY_STEP_5")
    key6 = os.environ.get("GEMINI_API_KEY_STEP_6")
    
    if not all([key4, key5, key6]):
        print("Error: All 3 API keys (STEP_4, STEP_5, STEP_6) must be set in .env.local")
        sys.exit(1)

    client4 = genai.Client(api_key=key4)
    client5 = genai.Client(api_key=key5)
    client6 = genai.Client(api_key=key6)

    existing_results = load_results()
    updated_results = {r["id"]: r for r in existing_results}
    
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    rows_to_process = []
    for row in rows:
        rid = row["id"]
        
        # If --id is provided, only process that ID
        if args.id and rid != args.id:
            continue

        if str(row.get("valid_url", "")).upper() != "TRUE": continue
        if not args.repeat and not args.id and rid in updated_results and "description" in updated_results[rid]: continue
        if not os.path.exists(os.path.join(MD_DIR, rid, "main0.md")): continue
        rows_to_process.append(row)

    if args.limit and args.limit < len(rows_to_process):
        rows_to_process = rows_to_process[:args.limit]
        print(f"Orchestrator: Limiting to first {args.limit} rows...")
    else:
        print(f"Orchestrator: Processing all {len(rows_to_process)} remaining rows...")

    try:
        from tqdm import tqdm
        iterator = tqdm(rows_to_process, desc="Steps 4-6 (Parallel)", unit="row")
    except ImportError: iterator = rows_to_process

    for row in iterator:
        rid = row["id"]
        content = get_combined_md_content(rid)
        if not content: continue

        try:
            with ThreadPoolExecutor(max_workers=3) as executor:
                f_dates = executor.submit(call_gemini, client4, PROMPT_DATES, content, "step4_extract_dates")
                f_meta = executor.submit(call_gemini, client5, PROMPT_METADATA, content, "step5_extract_metadata")
                f_desc = executor.submit(call_gemini, client6, PROMPT_DESCRIPTION, content, "step6_extract_description")
                
                # Use a dictionary to track which step is which
                futures = {
                    f_dates: "step4_extract_dates",
                    f_meta: "step5_extract_metadata",
                    f_desc: "step6_extract_description"
                }
                
                results = {}
                for future in futures:
                    step_name = futures[future]
                    try:
                        results[step_name] = future.result()
                        clear_failure(rid, step_name)
                    except Exception as e:
                        print(f"    {step_name} failed for {rid}: {e}")
                        log_failure(rid, step_name, e)
                        results[step_name] = {}

            res_dates = results.get("step4_extract_dates", {})
            res_meta = results.get("step5_extract_metadata", {})
            res_desc = results.get("step6_extract_description", {})

            # Merge results
            entry = {
                "id": rid,
                "name": row.get("title", ""),
                "url": row.get("url", ""),
                "deadlines_found": str(res_dates.get("deadlines_found", False)).lower(),
                "program_dates_found": str(res_dates.get("program_dates_found", False)).lower(),
                **res_dates,
                **res_meta,
                **res_desc
            }
            
            updated_results[rid] = entry
            # CLEAR Orchestrator-level failure on success
            clear_failure(rid, "step4_6_orchestrator")
            save_results(list(updated_results.values()))
            
        except Exception as e:
            print(f"Orchestrator error on {rid}: {e}")
            log_failure(rid, "step4_6_orchestrator", e)

        time.sleep(1) # Small delay to stay under RPM limits

    print("Orchestrator complete.")

if __name__ == "__main__":
    main()

