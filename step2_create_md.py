import csv
import os
import sys
import re
import time
import json
import tempfile
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup, Comment
from markdownify import markdownify as md
from google import genai
from google.genai import types
from dotenv import load_dotenv
from failed_tracker import log_failure, clear_failure
from pdf_tracker import get_pdf_ids, remove_pdf_id
from token_logger import log_tokens

# Load environment variables
load_dotenv('.env.local')

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

INPUT_FILE = 'ec_bank_rows_with_valid_url.csv'
OUTPUT_DIR = 'md-files'
RESULTS_FILE = 'results.json'
MAX_WORKERS = 10
TIMEOUT_SECONDS = 30
MODEL_NAME = "gemini-3-flash-preview"

PDF_PROMPT = """
You are a document transcription assistant. I am providing a PDF file that contains information about a summer program. 
Please read the entire PDF and provide a clean, structured Markdown transcription of the content. 
Focus on details like application deadlines, program dates, eligibility, costs, and program descriptions. 
Output ONLY the Markdown content.
"""

def load_existing_result_ids():
    """Loads IDs already present in results.json."""
    if not os.path.exists(RESULTS_FILE):
        return set()
    try:
        with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            results = data.get('results', [])
            return {r['id'] for r in results if 'id' in r}
    except (json.JSONDecodeError, FileNotFoundError):
        return set()

def clean_markdown(text):
    """
    Post-process Markdown to collapse multiple newlines.
    """
    if not text:
        return ""
    # Collapse 3 or more newlines into 2
    return re.sub(r'\n{3,}', '\n\n', text)

def fetch_and_convert(url, client=None):
    """
    Fetches the URL and converts the FULL content to Markdown.
    Handles both HTML and PDF formats using Gemini for PDF.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Clean the URL of trailing punctuation that might break requests
    url = url.strip().rstrip('.')
    
    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        
        # MAGIC BYTE DETECTION
        is_pdf = response.content.startswith(b'%PDF')
        
        if (is_pdf or 'application/pdf' in response.headers.get('Content-Type', '').lower()) and client:
            print(f"    Detected PDF content for {url}. Using Gemini to transcribe...")
            
            # Use Gemini to transcribe PDF
            response_gemini = client.models.generate_content(
                model=MODEL_NAME,
                contents=[
                    types.Part.from_bytes(data=response.content, mime_type="application/pdf"),
                    PDF_PROMPT
                ]
            )
            
            # Log tokens
            if response_gemini.usage_metadata:
                log_tokens("step2_create_md_pdf", 
                           MODEL_NAME,
                           response_gemini.usage_metadata.prompt_token_count, 
                           response_gemini.usage_metadata.candidates_token_count)
            
            return clean_markdown(response_gemini.text)

        # Use BeautifulSoup for HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg', 'meta', 'link']):
            tag.decompose()
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        markdown_content = md(str(soup), heading_style="ATX", strip=['img'])
        return clean_markdown(markdown_content)
        
    except Exception as e:
        raise e

def process_row(row, client=None):
    """
    Fetch and convert a single row's URL to Markdown.
    Returns (success: bool, message: str)
    """
    row_id = row.get('id')
    url = row.get('url')
    
    if not row_id or not url:
        return False, "Missing ID or URL"

    # Prepare directory
    row_dir = os.path.join(OUTPUT_DIR, row_id)
    output_file = os.path.join(row_dir, 'main0.md')
    sources_path = os.path.join(row_dir, 'sources.json')
    
    # Ensure directory exists
    os.makedirs(row_dir, exist_ok=True)

    # Load existing sources or start fresh
    sources = {"files": {}, "errors": {}}
    if os.path.exists(sources_path):
        try:
            with open(sources_path, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
                if "files" in old_data:
                    sources = old_data
                else:
                    # Migrate old flat format
                    sources["files"] = old_data
        except: pass

    try:
        markdown_content = fetch_and_convert(url, client)
        
        if not markdown_content or not markdown_content.strip():
             raise ValueError(f"Empty content extracted from {url}")

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
            
        sources["files"]['main0.md'] = url
        # Remove from errors if it was there
        if 'main0.md' in sources["errors"]:
            del sources["errors"]['main0.md']
            
        with open(sources_path, 'w', encoding='utf-8') as f:
            json.dump(sources, f, indent=2)
            
        clear_failure(row_id, "step2_create_md")
        # Remove from pdf_to_clean if it was there
        remove_pdf_id(row_id)
        return True, f"Saved {row_id}"

    except Exception as e:
        # Update sources.json on failure
        sources["errors"]["main0.md"] = str(e)
        if "main0.md" in sources["files"]:
            del sources["files"]["main0.md"]
        
        # Ensure stale file is removed if it failed
        if os.path.exists(output_file):
            os.remove(output_file)
            
        try:
            with open(sources_path, 'w', encoding='utf-8') as f:
                json.dump(sources, f, indent=2)
        except: pass

        log_failure(row_id, "step2_create_md", e)
        return False, f"Error processing {url}: {str(e)}"

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created directory: {OUTPUT_DIR}")

    # Read CSV
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        print(f"Error: Could not find input file '{INPUT_FILE}'")
        sys.exit(1)

    # Filter for valid URLs
    valid_rows = [
        row for row in rows 
        if str(row.get('valid_url', '')).strip().upper() == 'TRUE'
    ]

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows to process")
    parser.add_argument("--repeat", action="store_true", help="Reprocess all rows even if they exist")
    parser.add_argument("--id", type=str, default=None, help="Process only this specific ID")
    parser.add_argument("--api-key", type=str, default=os.environ.get("GEMINI_API_KEY"), help="Google Gemini API Key")
    # Parse known args only, in case other args are passed that we don't care about (though not expected)
    args, _ = parser.parse_known_args()

    # If --id is provided, filter valid_rows to only that ID
    if args.id:
        valid_rows = [row for row in valid_rows if row.get('id') == args.id]
        if not valid_rows:
            print(f"Error: ID {args.id} not found or has no valid URL.")
            return

    # If not repeat, check existing MD directories and results.json
    pdf_fix_ids = get_pdf_ids()
    
    if not args.repeat:
        existing_result_ids = load_existing_result_ids()
        filtered_rows = []
        for row in valid_rows:
            row_id = row['id']
            
            # If it's in the PDF cleanup list, DO NOT SKIP IT
            if row_id in pdf_fix_ids:
                filtered_rows.append(row)
                continue

            # Skip if ID is in results.json OR if MD file already exists
            if row_id in existing_result_ids:
                continue
            if os.path.exists(os.path.join(OUTPUT_DIR, row_id, 'main0.md')):
                continue
            filtered_rows.append(row)
        
        skipped_count = len(valid_rows) - len(filtered_rows)
        valid_rows = filtered_rows
        print(f"Skipping {skipped_count} existing rows (repeat=False).")

    if args.limit and args.limit < len(valid_rows):
        valid_rows = valid_rows[:args.limit]
        print(f"Limiting to first {args.limit} rows.")
    else:
        print(f"Processing all {len(valid_rows)} remaining valid rows.")

    total_valid = len(valid_rows)
    print(f"Found {len(rows)} total rows. Processing {total_valid} valid URLs with {MAX_WORKERS} workers...")

    success_count = 0
    error_count = 0
    
    # Use tqdm if available
    pbar = tqdm(total=total_valid, unit="url") if tqdm else None

    if args.api_key:
        client = genai.Client(api_key=args.api_key)
    else:
        client = None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_row = {executor.submit(process_row, row, client): row for row in valid_rows}
        
        for future in as_completed(future_to_row):
            success, msg = future.result()
            
            if success:
                success_count += 1
            else:
                error_count += 1
                # Optional: print error if verbose
                # print(f"Failed: {msg}")

            if pbar:
                pbar.update(1)
            elif (success_count + error_count) % 10 == 0:
                print(f"Processed {success_count + error_count}/{total_valid}...")

    if pbar:
        pbar.close()

    print("-" * 40)
    print(f"Processing complete.")
    print(f"Success: {success_count}")
    print(f"Errors/Empty: {error_count}")

if __name__ == "__main__":
    main()
