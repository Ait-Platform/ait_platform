import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

text = text.replace('from app.pdf.generator import generate_pdf_from_html', 'from app.pdf.routes import html_to_pdf_bytes')
text = text.replace('generate_pdf_from_html(html_out)', 'html_to_pdf_bytes(html_out)')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
