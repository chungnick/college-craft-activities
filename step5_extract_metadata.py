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
parser = argparse.ArgumentParser(description="Extract metadata from MD files using Gemini.")
parser.add_argument("--limit", type=int, default=None, help="Number of rows to process")
parser.add_argument("--api-key", type=str, default=os.environ.get("GEMINI_API_KEY"), help="Google Gemini API Key")
parser.add_argument("--repeat", action="store_true", help="Reprocess all rows even if they exist")


# Configuration
MD_DIR = "md-files"
RESULTS_FILE = "results.json"
MODEL_NAME = "gemini-3-flash-preview"

# Options content (injected directly into prompt to avoid parsing the python file dynamically if possible, 
# or we can read it. Reading text of options.py is safer to keep it strict.)
def read_options_file():
    try:
        with open("options.py", "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

OPTIONS_CONTENT = read_options_file()

SYSTEM_PROMPT = f"""
You are a data extraction assistant. extracting structured metadata from a summer program's markdown page.

Use the following strict options for classification. If a field corresponds to one of these lists, you MUST choose values ONLY from the provided options.

{OPTIONS_CONTENT}

Extract the following fields:
1. mode: (Choose from options provided above)
2. price: (Choose from options provided above - e.g. Free or Paid)
3. eligibility: (Choose from options provided above)
4. grade_level: (Choose from options provided above)
5. location: (Choose from options provided above - e.g. State names or International)
6. details: (Choose from options provided above - Subject areas)
7. financial_aid: (Yes/No based on options)
8. application_requirements: (Choose from options provided above)

Format the output as a valid JSON object. 
If a field allows multi-select (as defined in options above), return a list of strings.
If a field is single select, return a single string (or null if not found).
If information is missing, use empty list [] for multi-select or null for single-select.

Example Output Structure:
{{
  "mode": "In-Person",
  "price": "Paid",
  "eligibility": ["Domestic Students"],
  "grade_level": ["10th", "11th"],
  "location": ["Pennsylvania"],
  "details": ["STEM", "Engineering"],
  "financial_aid": "Yes",
  "application_requirements": ["Essay", "Transcript"]
}}
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
    for i in range(1, 4):
        sister_path = os.path.join(MD_DIR, row_id, f"sister{i}.md")
        if os.path.exists(sister_path):
            with open(sister_path, "r", encoding="utf-8") as f:
                combined_content += f"--- SISTER PAGE {i} CONTENT ---\n{f.read()}\n\n"
    
    return combined_content

def extract_metadata(content: str, client: genai.Client) -> Dict:
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

            print(f"Error extracting metadata: {e}")
            return {}
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
        print("Error: Gemini API key is required.")
        sys.exit(1)

    client = genai.Client(api_key=args.api_key)

    existing_results = load_results()
    if not existing_results:
        print("No results.json found. Please run step3 first to populate initial data.")
        sys.exit(1)

    updated_results = {r["id"]: r for r in existing_results}
    
    # Filter for rows that have MD files
    rows_to_process = []
    for r in existing_results:
        if not args.repeat and "mode" in r: # Check a key from this step to see if processed
             continue
             
        if os.path.exists(os.path.join(MD_DIR, r["id"], "main0.md")):
            rows_to_process.append(r)
    
    print(f"Found {len(rows_to_process)} rows to process in results.json.")

    if args.limit:
        rows_to_process = rows_to_process[:args.limit]
        print(f"Limiting to first {args.limit} rows.")

    processed_count = 0
    
    # Import tqdm for progress bar
    try:
        from tqdm import tqdm
        iterator = tqdm(rows_to_process, desc="Extracting Metadata", unit="row")
    except ImportError:
        iterator = rows_to_process
        print("tqdm not installed, running without progress bar.")

    for row in iterator:
        row_id = row["id"]
        # print(f"Processing metadata for {row_id}...")
        
        content = get_combined_md_content(row_id)
        if not content:
            continue

        metadata = extract_metadata(content, client)
        
        # Merge metadata into the row
        updated_results[row_id].update(metadata)
        processed_count += 1
        
        time.sleep(2)

    final_results_list = list(updated_results.values())
    save_results(final_results_list)
    print(f"Step 4 complete. Processed {processed_count} rows.")

if __name__ == "__main__":
    main()

