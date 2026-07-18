import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement = '''
    html = render_template("program_billing/consumption_table_pdf.html", property=prop, month=month, data=data)
    
    try:
        pdf_bytes = html_to_pdf_bytes(html, orientation="Landscape")
'''

text = re.sub(
    r'html = render_template\("program_billing/consumption_table\.html", property=prop, month=month, data=data\).*?try:\s+pdf_bytes = html_to_pdf_bytes\(html\)',
    replacement.strip(),
    text,
    flags=re.DOTALL
)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("Updated route to use PDF template and Landscape orientation")
