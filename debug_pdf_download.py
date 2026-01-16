import requests
import fitz
import os

url = "https://edge.sitecorecloud.io/unichicagomc-81nbqnb3/media/pdfs/adult-pdfs/conditions-and-services/cancer/pathwayprograms2026_recommendation_instructions.pdf"
headers = {'User-Agent': 'Mozilla/5.0'}

print(f"Downloading {url}...")
try:
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    print(f"Downloaded {len(response.content)} bytes.")
    
    if response.content.startswith(b'%PDF'):
        print("Starts with %PDF magic bytes.")
    else:
        print("Does NOT start with %PDF magic bytes.")
        print(f"First 20 bytes: {response.content[:20]}")

    with open("debug_test.pdf", "wb") as f:
        f.write(response.content)
    
    doc = fitz.open("debug_test.pdf")
    print(f"Page count: {doc.page_count}")
    
    for i in range(doc.page_count):
        page = doc[i]
        text = page.get_text()
        print(f"Page {i+1} text length: {len(text)}")
        if len(text) < 100:
            print(f"Page {i+1} content preview: {text[:100]}")
            images = page.get_images()
            print(f"Page {i+1} image count: {len(images)}")
    
    doc.close()
except Exception as e:
    print(f"Error: {e}")
finally:
    if os.path.exists("debug_test.pdf"):
        os.remove("debug_test.pdf")

