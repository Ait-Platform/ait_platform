import re

with open('templates/program_billing/consumption_table.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Remove the extends and blocks
text = re.sub(r'\{% extends.*?%\}', '', text)
text = re.sub(r'\{% block.*?%\}', '', text)
text = re.sub(r'\{% endblock %\}', '', text)

# Wrap in a basic HTML structure with Tailwind CDN
pdf_html = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Consumption Review - {{ property.name }} - {{ month }}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: white; }
        .min-h-screen { min-height: auto !important; padding: 0 !important; }
        .shadow { box-shadow: none !important; border: none !important; }
        .border-slate-200 { border-color: #e2e8f0; }
        @media print {
            .page-break { page-break-inside: avoid; }
        }
    </style>
</head>
<body class="bg-white p-4">
''' + text.strip() + '''
</body>
</html>
'''

# Remove the Back button block and the Print/Email buttons
pdf_html = re.sub(r'<a href="\{\{ url_for.*?Back to Utilities Hub.*?</a>', '', pdf_html, flags=re.DOTALL)
pdf_html = re.sub(r'<div class="flex justify-end space-x-3 mb-6">.*?</div>', '', pdf_html, flags=re.DOTALL)
pdf_html = re.sub(r'<script>.*?</script>', '', pdf_html, flags=re.DOTALL)

with open('templates/program_billing/consumption_table_pdf.html', 'w', encoding='utf-8') as f:
    f.write(pdf_html)

print('Created consumption_table_pdf.html')
