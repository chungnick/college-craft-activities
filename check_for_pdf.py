import os

MD_DIR = "md-files"
OUTPUT_FILE = "pdf_to_clean.txt"

def is_likely_pdf(file_path):
    """
    Heuristic to determine if a markdown file was likely converted from a PDF.
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            
        line_count = len(lines)
        # 1. High line count is a strong indicator for PDF data dumps
        if line_count > 1000:
            return True
            
        content = "".join(lines)
        
        # 2. Check for the replacement character artifact common in PDF scraping
        if "" in content:
            return True
            
        # 3. Check for PDF-like artifacts (fragmented lines, page indicators)
        page_indicators = ["Page 1 of", "Page 2 of", "[1]", "[2]"]
        if any(indicator in content for indicator in page_indicators) and line_count > 300:
            return True
            
        return False
    except Exception:
        return False

def main():
    if not os.path.exists(MD_DIR):
        print(f"Error: {MD_DIR} not found.")
        return

    pdf_ids = set()
    
    print("Scanning folders for likely PDF conversions...")
    
    for folder_id in os.listdir(MD_DIR):
        folder_path = os.path.join(MD_DIR, folder_id)
        if not os.path.isdir(folder_path):
            continue
            
        for filename in os.listdir(folder_path):
            if filename.endswith(".md"):
                file_path = os.path.join(folder_path, filename)
                if is_likely_pdf(file_path):
                    pdf_ids.add(folder_id)
                    break # One file is enough to flag the folder
                    
    # Write to output file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for rid in sorted(list(pdf_ids)):
            f.write(f"{rid}\n")
            
    print(f"Scan complete. Found {len(pdf_ids)} folders with likely PDF content.")
    print(f"IDs written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

