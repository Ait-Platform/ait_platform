import re

file_path = 'app/pdf/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

import_block = '''import sys, io
import pdfkit

try:
    from weasyprint import HTML
    WEASYPRINT_AVAILABLE = True
except Exception:
    WEASYPRINT_AVAILABLE = False'''

text = text.replace('''import sys, io
import pdfkit

try:
    from weasyprint import HTML
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False''', import_block)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
