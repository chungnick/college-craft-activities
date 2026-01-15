import csv
import os
import sys
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Try importing required libraries
try:
    import requests
    from bs4 import BeautifulSoup, Comment
    from markdownify import markdownify as md
except ImportError as e:
    print(f"Error: Required library missing: {e}")
    print("Please install requirements: pip install requests beautifulsoup4 markdownify")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

INPUT_FILE = 'ec_bank_rows_with_valid_url.csv'
OUTPUT_DIR = 'md-files'
MAX_WORKERS = 10
TIMEOUT_SECONDS = 30

def clean_markdown(text):
    """
    Post-process Markdown to collapse multiple newlines.
    """
    if not text:
        return ""
    # Collapse 3 or more newlines into 2
    return re.sub(r'\n{3,}', '\n\n', text)

def fetch_and_convert(url):
    """
    Fetches the URL and converts the FULL content to Markdown.
    Removes script/style tags but keeps all other structural elements.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        
        # Use BeautifulSoup to clean the DOM before conversion
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove unwanted tags that contain code or non-visible content
        # We keep header, footer, nav, div, span, etc. for completeness
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg', 'meta', 'link']):
            tag.decompose()

        # Remove HTML comments
        for comment in soup.find_all(text=lambda text: isinstance(text, Comment)):
            comment.extract()

        # Convert to Markdown
        # heading_style="ATX" uses # for headers instead of underlining
        markdown_content = md(str(soup), heading_style="ATX", strip=['img']) # Keep links by default (strip only img)
        
        return clean_markdown(markdown_content)
        
    except Exception as e:
        raise e

def process_row(row):
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
    
    # Ensure directory exists
    os.makedirs(row_dir, exist_ok=True)

    try:
        markdown_content = fetch_and_convert(url)
        
        if not markdown_content or not markdown_content.strip():
             return False, f"Empty content extracted from {url}"

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
            
        return True, f"Saved {row_id}"

    except Exception as e:
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
    # Parse known args only, in case other args are passed that we don't care about (though not expected)
    args, _ = parser.parse_known_args()

    # If not repeat, check existing MD directories
    if not args.repeat:
        # We check if output directory for the ID exists and has main0.md
        # This prevents re-fetching URLs that are already downloaded.
        filtered_rows = []
        for row in valid_rows:
            row_id = row['id']
            if not os.path.exists(os.path.join(OUTPUT_DIR, row_id, 'main0.md')):
                filtered_rows.append(row)
        valid_rows = filtered_rows
        print(f"Skipping {len(rows) - len(valid_rows)} existing rows (repeat=False).")

    if args.limit:
        valid_rows = valid_rows[:args.limit]
        print(f"Limiting to first {args.limit} rows.")

    total_valid = len(valid_rows)
    print(f"Found {len(rows)} total rows. Processing {total_valid} valid URLs with {MAX_WORKERS} workers...")

    success_count = 0
    error_count = 0
    
    # Use tqdm if available
    pbar = tqdm(total=total_valid, unit="url") if tqdm else None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_row = {executor.submit(process_row, row): row for row in valid_rows}
        
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
