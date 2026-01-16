import csv
import json
import os
import sys
from urllib.parse import urlparse

# Configuration
EC_BANK_ROWS_FILE = "ec_bank_rows.csv"
RESULTS_FILE = "results.json"

def normalize_url(url):
    """
    Normalize URL for comparison:
    - Lowercase
    - Strip whitespace
    - Remove trailing slash
    - Remove scheme (http/https) to focus on domain/path
    """
    if not url:
        return ""
    
    u = url.strip().lower()
    
    # Remove trailing slash
    if u.endswith('/'):
        u = u[:-1]
        
    # Parse to remove scheme
    try:
        parsed = urlparse(u)
        # Reconstruct without scheme
        # netloc + path + params + query + fragment
        # If scheme is missing, netloc might be empty and path contains the url
        cleaned = parsed.netloc + parsed.path
        if parsed.query:
            cleaned += "?" + parsed.query
        if not cleaned: # if url was just a path or something weird
            cleaned = u
    except:
        cleaned = u
        
    return cleaned

def load_csv_urls(filepath):
    urls = set()
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found.")
        return urls
    
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("url")
            if url:
                urls.add(normalize_url(url))
    return urls

def load_json_urls(filepath):
    urls = set()
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found.")
        return urls
        
    with open(filepath, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            # Handle list or dict wrapper
            rows = data.get("results", []) if isinstance(data, dict) else data
            
            for row in rows:
                url = row.get("url")
                if url:
                    urls.add(normalize_url(url))
        except json.JSONDecodeError:
            print(f"Error: {filepath} is not valid JSON.")
            
    return urls

def main():
    print("Loading URLs...")
    csv_urls = load_csv_urls(EC_BANK_ROWS_FILE)
    json_urls = load_json_urls(RESULTS_FILE)
    
    print(f"URLs in {EC_BANK_ROWS_FILE}: {len(csv_urls)}")
    print(f"URLs in {RESULTS_FILE}: {len(json_urls)}")
    
    in_csv_not_json = csv_urls - json_urls
    in_json_not_csv = json_urls - csv_urls
    
    print("\n" + "="*50)
    print(f"{'DELTA REPORT':^50}")
    print("="*50)
    
    print(f"\nIn {EC_BANK_ROWS_FILE} but NOT in {RESULTS_FILE}: {len(in_csv_not_json)}")
    if len(in_csv_not_json) > 0:
        print("-" * 20)
        for i, url in enumerate(list(in_csv_not_json)[:20]):
            print(f" - {url}")
        if len(in_csv_not_json) > 20:
            print(f" ... and {len(in_csv_not_json) - 20} more.")
            
    print(f"\nIn {RESULTS_FILE} but NOT in {EC_BANK_ROWS_FILE}: {len(in_json_not_csv)}")
    if len(in_json_not_csv) > 0:
        print("-" * 20)
        for i, url in enumerate(list(in_json_not_csv)[:20]):
            print(f" - {url}")
        if len(in_json_not_csv) > 20:
            print(f" ... and {len(in_json_not_csv) - 20} more.")
            
    print("\n" + "="*50)

if __name__ == "__main__":
    main()

