import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

text = text.replace('from app.pdf.routes import html_to_pdf_bytes', 'from app.utils.pdf_render import html_to_pdf_bytes')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
