import os
import sys
import argparse
import time
import json
import requests
import pymupdf4llm
import fitz
import tempfile
from bs4 import BeautifulSoup, Comment
from markdownify import markdownify as md
from typing import Dict, List, Optional
from google import genai
from google.genai import types
from dotenv import load_dotenv
from token_logger import log_tokens
from failed_tracker import log_failure, clear_failure
from pdf_tracker import get_pdf_ids, remove_pdf_id

# Load environment variables
load_dotenv('.env.local')

# Setup argument parser
parser = argparse.ArgumentParser(description="Find and fetch 'sister' pages for more info.")
parser.add_argument("--limit", type=int, default=None, help="Number of rows to process")
parser.add_argument("--api-key", type=str, default=os.environ.get("GEMINI_API_KEY"), help="Google Gemini API Key")
parser.add_argument("--repeat", action="store_true", help="Reprocess all rows even if they exist")
parser.add_argument("--id", type=str, default=None, help="Process only this specific ID")

# Configuration
MD_DIR = "md-files"
RESULTS_FILE = "results.json"
MODEL_NAME = "gemini-2.5-flash-lite-preview-09-2025"
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
Select ONLY the most promising URLs (UP TO 3, but fewer is better if the information is likely redundant). 
Prioritize links like "Program Details", "Application Requirements", "FAQ", "Dates & Deadlines", or "Tuition".

CRITICAL - DO NOT SELECT:
- Social media links (Facebook, Twitter, Instagram, etc.)
- Generic homepage links
- Mailto links
- Links related to: "Contact Us", "About Us", "Login", "Sign In", "Register", "Create Account", "Privacy Policy", "Terms of Service", "Accessibility".
- Links that likely point to external registration systems (e.g., CampDoc, Circuitree) unless they are the only source of info.

Return a JSON object with a key "sister_urls" containing a list of strings.
Example:
{
  "sister_urls": [
    "https://example.com/program-dates",
    "https://example.com/application-info"
  ]
}
"""

PDF_PROMPT = """
You are a document transcription assistant. I am providing a PDF file that contains information about a summer program. 
Please read the entire PDF and provide a clean, structured Markdown transcription of the content. 
Focus on details like application deadlines, program dates, eligibility, costs, and program descriptions. 
Output ONLY the Markdown content.
"""

def clean_markdown(text):
    import re
    if not text: return ""
    return re.sub(r'\n{3,}', '\n\n', text)

def fetch_and_convert(url, client=None):
    """
    Fetches the URL and converts to Markdown.
    Handles both HTML and PDF formats using Gemini for PDF.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Clean the URL of trailing punctuation
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
                log_tokens("step3_sister_md_pdf", 
                           MODEL_NAME,
                           response_gemini.usage_metadata.prompt_token_count, 
                           response_gemini.usage_metadata.candidates_token_count)
            
            return clean_markdown(response_gemini.text)

        soup = BeautifulSoup(response.content, 'html.parser')
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg', 'meta', 'link']):
            tag.decompose()
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
            
        markdown_content = md(str(soup), heading_style="ATX", strip=['img'])
        return clean_markdown(markdown_content)
    except Exception as e:
        raise e

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
        
        # Log tokens
        if response.usage_metadata:
            log_tokens("step3_sister_md", 
                       MODEL_NAME,
                       response.usage_metadata.prompt_token_count, 
                       response.usage_metadata.candidates_token_count)

        if response.text:
             data = json.loads(response.text)
             urls = data.get("sister_urls", [])
             # Hardcoded secondary filter for common junk keywords
             junk_keywords = ['facebook', 'twitter', 'instagram', 'linkedin', 'login', 'signup', 'register', 'privacy-policy', 'terms-of-service', 'accessibility', 'mailto:']
             filtered_urls = [u for u in urls if not any(k in u.lower() for k in junk_keywords)]
             return filtered_urls
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
    
    # Load IDs already in results.json
    existing_result_ids = {r["id"] for r in existing_results if "id" in r}
    pdf_fix_ids = get_pdf_ids()
    
    rows_to_process = []
    for row in csv_rows:
        row_id = row["id"]
        
        # If --id is provided, only process that ID
        if args.id and row_id != args.id:
            continue

        if str(row.get("valid_url", "")).strip().upper() != "TRUE":
            continue
            
        # If it's in the PDF cleanup list, DO NOT SKIP IT
        if row_id in pdf_fix_ids:
            rows_to_process.append(row)
            continue

        # Skip if ID is in results.json
        if not args.repeat and not args.id and row_id in existing_result_ids:
            continue
            
        main_md_path = os.path.join(MD_DIR, row_id, "main0.md")
        if not os.path.exists(main_md_path):
            continue
            
        sister1_path = os.path.join(MD_DIR, row_id, "sister1.md")
        if not args.repeat and not args.id and os.path.exists(sister1_path):
             continue

        rows_to_process.append(row)
        
    print(f"Found {len(rows_to_process)} rows to process.")


    if args.limit and args.limit < len(rows_to_process):
        rows_to_process = rows_to_process[:args.limit]
        print(f"Limiting to first {args.limit} rows.")
    else:
        print(f"Processing all {len(rows_to_process)} remaining rows.")

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

        try:
            # 1. Ask Gemini for likely useful links
            sister_urls = identify_sister_links(content, client)
            
            # Load existing sources or start fresh
            sources_path = os.path.join(MD_DIR, row_id, "sources.json")
            sources = {"files": {"main0.md": row.get("url", "")}, "errors": {}}
            if os.path.exists(sources_path):
                try:
                    with open(sources_path, "r") as f:
                        old_sources = json.load(f)
                        if "files" in old_sources: sources = old_sources
                except: pass

            if sister_urls:
                # 2. Fetch and save these URLs
                for i, url in enumerate(sister_urls[:3]):
                    filename = f"sister{i+1}.md"
                    final_url = url
                    if not url.startswith('http'):
                        base_url = row.get('url', '')
                        if base_url:
                            from urllib.parse import urljoin
                            final_url = urljoin(base_url, url)
                    
                    print(f"    Fetching sister {i+1}: {final_url}")
                    
                    try:
                        sister_md = fetch_and_convert(final_url, client)
                        if not sister_md or not sister_md.strip():
                            raise ValueError(f"Empty content extracted from {final_url}")
                            
                        out_path = os.path.join(MD_DIR, row_id, filename)
                        with open(out_path, "w", encoding="utf-8") as f:
                            f.write(sister_md)
                        sources["files"][filename] = final_url
                        # Remove from errors if it was there previously
                        if filename in sources["errors"]: del sources["errors"][filename]
                    except Exception as fetch_err:
                        sources["errors"][filename] = str(fetch_err)
                        # If it failed, ensure any old successful file is removed to stay in sync
                        old_file_path = os.path.join(MD_DIR, row_id, filename)
                        if os.path.exists(old_file_path): os.remove(old_file_path)
                    
                    time.sleep(1)
            
            # Save enriched sources.json
            with open(sources_path, "w", encoding="utf-8") as f:
                json.dump(sources, f, indent=2)
            
            # CLEAR FAILURE on success (even if no sister links found, the process worked)
            clear_failure(row_id, "step3_sister_md")
            # Remove from pdf_to_clean if it was there
            remove_pdf_id(row_id)
            
        except Exception as e:
            print(f"    Failed row {row_id}: {e}")
            log_failure(row_id, "step3_sister_md", e)
            continue
        
        # Sleep to avoid rate limits
        time.sleep(2)

if __name__ == "__main__":
    main()

