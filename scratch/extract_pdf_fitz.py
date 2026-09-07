import fitz

doc = fitz.open('app/static/pdf/P_Guide.pdf')
for i in range(min(5, len(doc))):
    page = doc.load_page(i)
    text = page.get_text("text")
    print(f"--- PAGE {i+1} ---")
    lines = text.strip().split('\n')
    for line in lines[:5]:
        print(line.strip())
