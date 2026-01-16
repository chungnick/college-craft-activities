import fitz
import pymupdf4llm
import os

path = "md-files/f443c083-bd0c-4dd6-936d-5e4aead88aad/sister2.md"

print(f"File exists: {os.path.exists(path)}")
print(f"File size: {os.path.getsize(path)}")

try:
    print("Attempting to open with fitz via stream...")
    with open(path, "rb") as f:
        stream = f.read()
    doc = fitz.open(stream=stream, filetype="pdf")
    print(f"Pages: {doc.page_count}")
    doc.close()
    
    print("Attempting to convert with pymupdf4llm via path...")
    # pymupdf4llm doesn't support stream easily, so let's try path again or rename
    import shutil
    temp_pdf = "temp_debug.pdf"
    shutil.copy(path, temp_pdf)
    try:
        md_text = pymupdf4llm.to_markdown(temp_pdf)
        print(f"Extracted {len(md_text)} characters.")
    finally:
        if os.path.exists(temp_pdf):
            os.remove(temp_pdf)
except Exception as e:
    print(f"Error: {e}")

