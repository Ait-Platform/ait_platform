import sys
try:
    import PyPDF2
    with open(sys.argv[1], 'rb') as f:
        reader = PyPDF2.PdfReader(f)
        for page in reader.pages:
            print(page.extract_text())
except Exception as e:
    print(f"Error: {e}")
