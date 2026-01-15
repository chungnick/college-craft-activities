import json
import os
import sys
import argparse
import time
from typing import Dict, List, Optional
from google import genai
from google.genai import types
from dotenv import load_dotenv

# Load environment variables from .env.local
load_dotenv('.env.local')

# Setup argument parser
parser = argparse.ArgumentParser(description="Extract dates from MD files using Gemini.")
parser.add_argument("--limit", type=int, default=None, help="Number of rows to process")
parser.add_argument("--api-key", type=str, default=os.environ.get("GEMINI_API_KEY"), help="Google Gemini API Key")
parser.add_argument("--repeat", action="store_true", help="Reprocess all rows even if they exist")


# Configuration
INPUT_FILE = "ec_bank_rows_with_valid_url.csv" # To get the IDs
MD_DIR = "md-files"
RESULTS_FILE = "results.json"
# using gemini-1.5-flash which has a 1M token context window
MODEL_NAME = "gemini-3-flash-preview"

SYSTEM_PROMPT = """
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
    { "label": "Tech Crafters Session 2", "dates": ["YYYY-MM-DD", "YYYY-MM-DD"] }
    { "label": "Tech Crafters Session 3", "dates": ["YYYY-MM-DD", "YYYY-MM-DD"] }
  ],
  "program_dates_found": true
}

Note that it is possible the deadlines or program dates may not have labels.

"deadlines": [
    { "label": "", "dates": ["YYYY-MM-DD"] }
  ],

Rules:
- Dates should be in YYYY-MM-DD format.
- If a date range is given, include both start and end dates in the "dates" array.
- If a single date is given, the "dates" array will have one element.
- If no deadlines are found, set "deadlines_found" to false and "deadlines" to [].
- If no program dates are found, set "program_dates_found" to false and "program_dates" to [].
- "deadlines_found" and "program_dates_found" should be boolean values (true/false) in the JSON.

Response should contain ONLY the JSON object.
"""

def get_combined_md_content(row_id: str) -> Optional[str]:
    # Read main file
    main_path = os.path.join(MD_DIR, row_id, "main0.md")
    if not os.path.exists(main_path):
        return None
        
    combined_content = ""
    with open(main_path, "r", encoding="utf-8") as f:
        combined_content += f"--- MAIN PAGE CONTENT ---\n{f.read()}\n\n"

    # Read sister files
    for i in range(1, 4): # Check sister1.md, sister2.md, sister3.md
        sister_path = os.path.join(MD_DIR, row_id, f"sister{i}.md")
        if os.path.exists(sister_path):
            with open(sister_path, "r", encoding="utf-8") as f:
                combined_content += f"--- SISTER PAGE {i} CONTENT ---\n{f.read()}\n\n"
    
    return combined_content

def extract_dates(content: str, client: genai.Client) -> Dict:
    # Simple retry logic for 503 errors
    max_retries = 3
    base_delay = 5
    
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=f"{SYSTEM_PROMPT}\n\nInput Markdown:\n{content}",
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return json.loads(response.text)
        except Exception as e:
            if "503" in str(e) or "overloaded" in str(e).lower():
                if attempt < max_retries - 1:
                    sleep_time = base_delay * (attempt + 1)
                    print(f"    Model overloaded (503). Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                    continue
            
            print(f"Error extracting dates: {e}")
            return {
                "deadlines": [],
                "deadlines_found": "false",
                "program_dates": [],
                "program_dates_found": "false"
            }
    return {}

def load_results():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                return data.get("results", [])
            except json.JSONDecodeError:
                return []
    return []

def save_results(results: List[Dict]):
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump({"results": results}, f, indent=2)

def main():
    args = parser.parse_args()
    
    if not args.api_key:
        print("Error: Gemini API key is required. Set GEMINI_API_KEY in .env.local or pass --api-key.")
        sys.exit(1)

    client = genai.Client(api_key=args.api_key)

    # Load existing results to update or create new
    existing_results = load_results()
    
    # Read the input CSV to get IDs and valid_urls
    import csv
    processed_count = 0
    
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found.")
        sys.exit(1)

    # Filter for rows that have MD files generated
    rows_to_process = []
    
    existing_ids = set()
    if not args.repeat:
        # Check which IDs already have data in results for this step (or just exist in results)
        # For step 3, we check if 'deadlines_found' or 'program_dates_found' exists, 
        # but simpler to just check if ID is in results at all if we assume sequential running.
        # Better: check if the specific fields we extract are present.
        for r in existing_results:
             if "deadlines_found" in r:
                 existing_ids.add(r["id"])

    for row in rows:
        if str(row.get("valid_url", "")).strip().upper() == "TRUE":
             if row["id"] in existing_ids and not args.repeat:
                 continue
                 
             if os.path.exists(os.path.join(MD_DIR, row["id"], "main0.md")):
                 rows_to_process.append(row)

    print(f"Found {len(rows_to_process)} rows with MD files.")

    if args.limit:
        rows_to_process = rows_to_process[:args.limit]
        print(f"Limiting to first {args.limit} rows.")

    updated_results = {r["id"]: r for r in existing_results}

    # Import tqdm for progress bar
    try:
        from tqdm import tqdm
        iterator = tqdm(rows_to_process, desc="Extracting Dates", unit="row")
    except ImportError:
        iterator = rows_to_process
        print("tqdm not installed, running without progress bar.")

    for row in iterator:
        row_id = row["id"]
        # print(f"Processing {row_id}...")
        
        content = get_combined_md_content(row_id)
        if not content:
            # print(f"  No MD content found for {row_id}")
            continue

        extracted_data = extract_dates(content, client)
        
        # Normalize booleans to strings "true"/"false" as per user example
        deadlines_found = str(extracted_data.get("deadlines_found", False)).lower()
        program_dates_found = str(extracted_data.get("program_dates_found", False)).lower()

        result_entry = {
            "id": row_id,
            "name": row.get("title", ""), 
            "url": row.get("url", ""),
            "deadlines": extracted_data.get("deadlines", []),
            "deadlines_found": deadlines_found,
            "program_dates": extracted_data.get("program_dates", []),
            "program_dates_found": program_dates_found
        }
        
        updated_results[row_id] = result_entry
        processed_count += 1
        
        # Rate limiting (simple sleep)
        time.sleep(2)

    # Convert back to list
    final_results_list = list(updated_results.values())
    save_results(final_results_list)
    print(f"Processed {processed_count} rows. Results saved to {RESULTS_FILE}.")

if __name__ == "__main__":
    main()
