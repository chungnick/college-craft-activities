import os
import threading

PDF_FILE = "pdf_to_clean.txt"
_lock = threading.Lock()

def get_pdf_ids():
    """Reads all IDs from pdf_to_clean.txt."""
    if not os.path.exists(PDF_FILE):
        return set()
    with open(PDF_FILE, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def remove_pdf_id(row_id):
    """Thread-safe removal of an ID from pdf_to_clean.txt."""
    with _lock:
        if not os.path.exists(PDF_FILE):
            return
        
        with open(PDF_FILE, "r", encoding="utf-8") as f:
            ids = [line.strip() for line in f if line.strip()]
        
        if row_id in ids:
            ids.remove(row_id)
            with open(PDF_FILE, "w", encoding="utf-8") as f:
                for rid in ids:
                    f.write(f"{rid}\n")

