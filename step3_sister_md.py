import os
import sys
import argparse
import time
import json
import requests
from bs4 import BeautifulSoup, Comment
from markdownify import markdownify as md
from typing import Dict, List, Optional
from google import genai
from google.genai import types
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env.local')

# Setup argument parser
parser = argparse.ArgumentParser(description="Find and fetch 'sister' pages for more info.")
parser.add_argument("--limit", type=int, default=None, help="Number of rows to process")
parser.add_argument("--api-key", type=str, default=os.environ.get("GEMINI_API_KEY"), help="Google Gemini API Key")
parser.add_argument("--repeat", action="store_true", help="Reprocess all rows even if they exist")

# Configuration
MD_DIR = "md-files"
RESULTS_FILE = "results.json"
MODEL_NAME = "gemini-3-flash-preview"
TIMEOUT_SECONDS = 30

SYSTEM_PROMPT = """
You are a research assistant. Your goal is to identify URLs within a Markdown document that are most likely to contain MISSING details about a summer program.
The key missing details we are looking for are:
- Application Deadlines
- Program Dates (Start/End)
- Eligibility Criteria (Grade level, citizenship)
- Cost / Tuition / Financial Aid
- Application Requirements

Analyze the provided markdown content (which may contain [Link Text](url) links).
Select up to 3 URLs that seem most promising to contain this missing information (e.g., links like "Apply Now", "Program Details", "FAQ", "Dates & Deadlines").
Do NOT select social media links (Facebook, Twitter), generic homepage links if specific ones exist, or mailto links.

Return a JSON object with a key "sister_urls" containing a list of strings.
Example:
{
  "sister_urls": [
    "https://example.com/program-dates",
    "https://example.com/application-info"
  ]
}
"""

def clean_markdown(text):
    import re
    if not text: return ""
    return re.sub(r'\n{3,}', '\n\n', text)

def fetch_and_convert(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg', 'meta', 'link']):
            tag.decompose()
        for comment in soup.find_all(text=lambda text: isinstance(text, Comment)):
            comment.extract()
            
        markdown_content = md(str(soup), heading_style="ATX", strip=['img'])
        return clean_markdown(markdown_content)
    except Exception as e:
        print(f"    Failed to fetch/convert {url}: {e}")
        return None

def get_md_content(row_id: str) -> Optional[str]:
    path = os.path.join(MD_DIR, row_id, "main0.md")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    return None

def identify_sister_links(content: str, client: genai.Client) -> List[str]:
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=f"{SYSTEM_PROMPT}\n\nInput Markdown:\n{content}",
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        if response.text:
             data = json.loads(response.text)
             return data.get("sister_urls", [])
        else:
             return []
    except Exception as e:
        print(f"    Error identifying links: {e}")
        return []

def load_results():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                return data.get("results", [])
            except json.JSONDecodeError:
                return []
    return []

def main():
    args = parser.parse_args()
    if not args.api_key:
        print("Error: Gemini API key is required.")
        sys.exit(1)

    client = genai.Client(api_key=args.api_key)
    existing_results = load_results()
    
    # Filter for rows that have MD files
    rows_to_process = []
    
    # Check if we should skip based on repeat flag
    # We'll check if sister files already exist to decide skipping
    for r in existing_results:
        row_id = r["id"]
        main_md_path = os.path.join(MD_DIR, row_id, "main0.md")
        
        if not os.path.exists(main_md_path):
            continue

        # If not repeat, check if any sister file (sister1.md) exists
        # This is a heuristic; if sister1.md exists, we assume we ran this step.
        # But wait, step 3 is "Finding Sister Pages". If we ran it and found nothing, sister1.md won't exist.
        # We need a better way to mark completion or just let it run (it's fast if gemini call is skipped, but gemini call is the slow part).
        # We can check if we have data in results.json but step 3 doesn't write to results.json (it writes MD files).
        # Actually, maybe we should just rely on the main "valid_url" check or "main0.md" being present.
        # But user wants to limit rows.
        
        # If we have run this before, maybe we should just skip if *any* sister file exists?
        sister1_path = os.path.join(MD_DIR, row_id, "sister1.md")
        if not args.repeat and os.path.exists(sister1_path):
            # If sister1.md exists, we definitely ran it.
            continue
            
        # What if we ran it and found no sister links? We'd re-run it every time.
        # Ideally we'd have a flag in results.json "sister_search_completed": true.
        # But for now, let's just stick to the sister1 check, or process everything in the limit range 
        # that hasn't been processed.
        
        # Problem: The logic below filters `existing_results` which is loaded from `results.json`.
        # `results.json` is populated by step 3/4/5/6 scripts usually? No, step 3 just reads it.
        # Wait, step 3 reads `results.json` to get IDs? 
        # `results.json` is usually created/updated by step 4 (dates), 5 (metadata), 6 (details).
        # Step 3 (sister md) relies on `results.json` existing? 
        # If `results.json` is empty or missing rows (because step 4 hasn't run yet), `existing_results` might be empty.
        # `results.json` is originally created by... wait, `step0/1` creates a CSV.
        # `step2` creates MD files.
        # `step3` should probably read the CSV, not `results.json`, because `results.json` might not contain the rows yet if step 4 hasn't run.
        # The previous scripts (old step 3->4) created `results.json` from scratch if needed.
        # But `step3_sister_md` currently reads `results.json`.
        # If `results.json` only has 4 rows (from previous runs), and we want 20, step 3 only sees 4 rows.
        # It should read the CSV instead!
        
        pass

    # REWRITE: Read CSV instead of results.json to ensure we see all candidates
    import csv
    input_csv = "ec_bank_rows_with_valid_url.csv" # Hardcoded or passed arg?
    # step2 uses ec_bank_rows_with_valid_url.csv
    
    csv_rows = []
    if os.path.exists(input_csv):
         with open(input_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            csv_rows = list(reader)
    
    rows_to_process = []
    for row in csv_rows:
        if str(row.get("valid_url", "")).strip().upper() != "TRUE":
            continue
            
        row_id = row["id"]
        main_md_path = os.path.join(MD_DIR, row_id, "main0.md")
        if not os.path.exists(main_md_path):
            continue
            
        sister1_path = os.path.join(MD_DIR, row_id, "sister1.md")
        if not args.repeat and os.path.exists(sister1_path):
             continue

        rows_to_process.append(row)
        
    print(f"Found {len(rows_to_process)} rows to process.")


    if args.limit:
        rows_to_process = rows_to_process[:args.limit]
        print(f"Limiting to first {args.limit} rows.")

    # Import tqdm for progress bar
    try:
        from tqdm import tqdm
        iterator = tqdm(rows_to_process, desc="Finding Sister Pages", unit="row")
    except ImportError:
        iterator = rows_to_process
        print("tqdm not installed, running without progress bar.")

    for row in iterator:
        row_id = row["id"]
        content = get_md_content(row_id)
        if not content:
            continue

        # 1. Ask Gemini for likely useful links
        sister_urls = identify_sister_links(content, client)
        
        if not sister_urls:
            continue

        # 2. Fetch and save these URLs
        # Only take top 3
        for i, url in enumerate(sister_urls[:3]):
            # Resolve relative URLs if necessary? 
            # Ideally markdownify kept absolute, or we might need base_url.
            # Assuming absolute for now or Gemini returns valid absolute/relative.
            # We'll handle basic relative path reconstruction if row has original URL.
            
            final_url = url
            if not url.startswith('http'):
                # Try to join with base url
                base_url = row.get('url', '')
                if base_url:
                    from urllib.parse import urljoin
                    final_url = urljoin(base_url, url)
            
            print(f"    Fetching sister {i+1}: {final_url}")
            sister_md = fetch_and_convert(final_url)
            
            if sister_md:
                # Save as sister{i+1}.md
                out_path = os.path.join(MD_DIR, row_id, f"sister{i+1}.md")
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(sister_md)
                
                # Sleep briefly between requests
                time.sleep(1)
        
        # Sleep to avoid rate limits
        time.sleep(2)

if __name__ == "__main__":
    main()

