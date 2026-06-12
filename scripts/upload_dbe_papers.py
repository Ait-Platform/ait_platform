import os
import sys
import json
import glob
import time
from dotenv import load_dotenv
import google.generativeai as genai

# Load environment variables from .env in the root directory
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    print("Error: GEMINI_API_KEY not found in environment variables.")
    sys.exit(1)

genai.configure(api_key=api_key)

PAPERS_DIR = os.path.join(os.path.dirname(__file__), '..', 'app', 'data', 'dbe_papers')
URIS_FILE = os.path.join(PAPERS_DIR, 'uris.json')

def upload_papers():
    if not os.path.exists(PAPERS_DIR):
        print(f"Directory {PAPERS_DIR} does not exist.")
        return

    # Load existing URIs to avoid duplicate uploads
    existing_uris = {}
    if os.path.exists(URIS_FILE):
        try:
            with open(URIS_FILE, 'r') as f:
                existing_uris = json.load(f)
        except Exception:
            pass

    pdf_files = glob.glob(os.path.join(PAPERS_DIR, '*.pdf'))
    if not pdf_files:
        print(f"No PDF files found in {PAPERS_DIR}.")
        return

    print(f"Found {len(pdf_files)} PDF files.")
    
    updated_uris = existing_uris.copy()
    
    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        
        # Check if already uploaded
        if filename in updated_uris:
            print(f"Skipping {filename} (already uploaded: {updated_uris[filename]})")
            continue
            
        print(f"Uploading {filename} to Gemini File API...")
        try:
            uploaded_file = genai.upload_file(path=pdf_path, display_name=filename)
            
            # Wait for processing
            print(f"Uploaded as {uploaded_file.uri}. Waiting for processing...")
            file_info = genai.get_file(uploaded_file.name)
            while file_info.state.name == "PROCESSING":
                print(".", end="", flush=True)
                time.sleep(2)
                file_info = genai.get_file(uploaded_file.name)
                
            if file_info.state.name == "FAILED":
                print(f"\nFailed to process {filename}.")
            else:
                print(f"\nSuccess! Ready for use.")
                updated_uris[filename] = uploaded_file.uri
                
        except Exception as e:
            print(f"Error uploading {filename}: {e}")

    # Save URIs back to JSON
    with open(URIS_FILE, 'w') as f:
        json.dump(updated_uris, f, indent=4)
        
    print("\nUpload process complete. URIs saved to uris.json")

if __name__ == "__main__":
    upload_papers()
