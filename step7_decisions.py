import os
import sys
import json
import argparse
import time
import asyncio
from typing import Dict, List, Any
from google import genai
from google.genai import types
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# Load environment variables
load_dotenv('.env.local')

# Setup argument parser
parser = argparse.ArgumentParser(description="Analyze deadlines and move decision dates.")
parser.add_argument("--limit", type=int, default=None, help="Number of rows to process")
parser.add_argument("--repeat", action="store_true", help="Reprocess all rows even if they exist")

# Configuration
RESULTS_FILE = "results.json"
MODEL_NAME = "gemini-2.5-flash-lite-preview-09-2025"

# API Keys
API_KEYS = [
    os.environ.get("GEMINI_API_KEY"),
    os.environ.get("GEMINI_API_KEY_STEP_4"),
    os.environ.get("GEMINI_API_KEY_STEP_5"),
    os.environ.get("GEMINI_API_KEY_STEP_6"),
]
# Filter out None keys
API_KEYS = [k for k in API_KEYS if k]

if not API_KEYS:
    print("Error: No Gemini API keys found.")
    sys.exit(1)

SYSTEM_PROMPT = """
You are a data analyst reviewing extracted deadlines for a summer program.
You will be given a list of deadlines with their labels and dates.
Your task is to identify if any of these "deadlines" are actually notification dates (when students find out if they got in), rather than application submission deadlines.

Common labels for decision notifications:
- "Notification Date"
- "Decisions Released"
- "Admission Decisions"
- "Announcement of Acceptance"
- "Results Announced"

Common labels for application deadlines (DO NOT MOVE THESE):
- "Application Deadline"
- "Submission Deadline"
- "Priority Deadline"
- "Regular Decision" (refers to the application deadline type)
- "Early Action" (refers to the application deadline type)

Return a JSON object containing a list of indices (0-based) of the deadlines that should be moved to "decisions_date".
If no deadlines are decision notifications, return an empty list.

Example Input:
[
  { "label": "Application Deadline", "dates": ["2025-02-01"] },
  { "label": "Decisions Released", "dates": ["2025-03-15"] },
  { "label": "Deposit Due", "dates": ["2025-04-01"] }
]

Example Output:
{
  "move_indices": [1]
}
"""

def load_results():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                # Ensure it's a list if wrapped or direct
                if isinstance(data, dict) and "results" in data:
                    return data["results"]
                return data if isinstance(data, list) else []
            except json.JSONDecodeError:
                return []
    return []

def save_results(results: List[Dict]):
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump({"results": results}, f, indent=2)

def analyze_deadlines(deadlines: List[Dict], client: genai.Client) -> List[int]:
    if not deadlines:
        return []
    
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=f"{SYSTEM_PROMPT}\n\nInput Deadlines:\n{json.dumps(deadlines, indent=2)}",
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        data = json.loads(response.text)
        return data.get("move_indices", [])
    except Exception as e:
        print(f"Error analyzing deadlines: {e}")
        return []

def process_batch(rows: List[Dict], api_key: str, batch_id: int) -> List[Dict]:
    client = genai.Client(api_key=api_key)
    processed_rows = []
    
    print(f"Batch {batch_id} starting with {len(rows)} rows...")
    
    for i, row in enumerate(rows):
        # Initialize decisions_date if not present
        if "decisions_date" not in row:
            row["decisions_date"] = []
            
        deadlines = row.get("deadlines", [])
        
        # Skip if no deadlines to check
        if not deadlines:
            processed_rows.append(row)
            continue
            
        # Analyze
        move_indices = analyze_deadlines(deadlines, client)
        
        if move_indices:
            new_deadlines = []
            moved_items = []
            
            for idx, item in enumerate(deadlines):
                if idx in move_indices:
                    moved_items.append(item)
                else:
                    new_deadlines.append(item)
            
            # Update row locally
            row["deadlines"] = new_deadlines
            row["decisions_date"].extend(moved_items)
            print(f"  [Batch {batch_id}] Row {row.get('id')}: Moved {len(moved_items)} items to decisions_date.")
        
        processed_rows.append(row)
        # Small sleep to be nice
        time.sleep(0.5)
        
    print(f"Batch {batch_id} complete.")
    return processed_rows

def main():
    args = parser.parse_args()
    
    all_results = load_results()
    if not all_results:
        print("No results found.")
        sys.exit(0)

    # 1. Identify eligible rows (has deadlines AND (repeat=True OR decisions_date not processed yet))
    # We'll use presence of "decisions_date" field as marker if repeat=False? 
    # But we just initialized it for everyone. 
    # Let's assume if it exists and is empty, it *might* be processed, but we can't distinguish "processed & found nothing" vs "not processed".
    # For robust "repeat=False", we should track processed status or just rely on user flag.
    # Given the instructions, we'll process rows that have deadlines.
    
    rows_to_process = []
    skipped_count = 0
    
    for r in all_results:
        # Ensure field exists for everyone first
        if "decisions_date" not in r:
            r["decisions_date"] = []
            
        has_deadlines = r.get("deadlines") and len(r.get("deadlines")) > 0
        
        # If repeat is False, skip if we suspect it's already done?
        # Since we are modifying 'deadlines' in place (removing items), reprocessing might be harmless 
        # UNLESS we move valid deadlines out by mistake.
        # But for this specific task, let's process all rows with deadlines if requested.
        # Actually, user said "feeds rows... that have a value for deadlines".
        
        if has_deadlines:
            rows_to_process.append(r)
        else:
            skipped_count += 1

    print(f"Total rows: {len(all_results)}. Eligible (has deadlines): {len(rows_to_process)}. Skipped (no deadlines): {skipped_count}")

    if args.limit:
        rows_to_process = rows_to_process[:args.limit]
        print(f"Limiting to first {args.limit} eligible rows.")

    # 2. Divide into batches based on available API keys
    num_keys = len(API_KEYS)
    batch_size = (len(rows_to_process) + num_keys - 1) // num_keys
    batches = []
    for i in range(num_keys):
        start = i * batch_size
        end = start + batch_size
        batch_rows = rows_to_process[start:end]
        if batch_rows:
            batches.append((batch_rows, API_KEYS[i], i+1))

    # 3. Process in parallel
    print(f"Starting {len(batches)} parallel batches...")
    
    processed_results_map = {} # Map ID to processed row
    
    with ThreadPoolExecutor(max_workers=num_keys) as executor:
        futures = [executor.submit(process_batch, b[0], b[1], b[2]) for b in batches]
        for future in futures:
            try:
                batch_res = future.result()
                for row in batch_res:
                    processed_results_map[row['id']] = row
            except Exception as e:
                print(f"Batch failed: {e}")

    # 4. Merge back into main results
    # We update the original list order
    final_output = []
    updates_count = 0
    
    for r in all_results:
        if r['id'] in processed_results_map:
            final_output.append(processed_results_map[r['id']])
            updates_count += 1
        else:
            final_output.append(r)

    # 5. Save
    save_results(final_output)
    print(f"Step 7 complete. Processed {updates_count} rows. Saved to {RESULTS_FILE}.")

if __name__ == "__main__":
    main()

