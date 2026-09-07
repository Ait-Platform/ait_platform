from pypdf import PdfReader

reader = PdfReader('app/static/pdf/P_Guide.pdf')
print(f"Total pages: {len(reader.pages)}")

# Extract text from the first 5 pages to see the structure
for i in range(min(5, len(reader.pages))):
    print(f"\n--- PAGE {i+1} ---")
    page = reader.pages[i]
    text = page.extract_text()
    # Print the first few lines of each page to find the titles
    lines = text.split('\n')
    for line in lines[:10]:
        print(line.strip())
