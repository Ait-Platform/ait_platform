import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

pattern = r"    doc = SaceDocument\.query\.filter_by\(document_type=doc_type\)\.first\(\)\n    if not doc:\n        flash\(\"Document not found or not uploaded yet\.\", \"error\"\)\n        return redirect\(url_for\('sace_bp\.reading_hub'\)\)"

replacement = '''    doc = SaceDocument.query.filter_by(document_type=doc_type).first()
    # TESTING FIX: If doc is missing, log interaction anyway and use a fallback title
    doc_title = doc.title if doc else doc_type.replace('_', ' ').title()
    doc_url = doc.document_url if doc else ""'''

text = re.sub(pattern, replacement, text)

# Also fix the return render_template
pattern2 = r"    return render_template\(\n        \"program_sace/secure_viewer\.html\",\n        doc=doc\n    \)"
replacement2 = '''    return render_template(
        "program_sace/secure_viewer.html",
        doc_title=doc_title,
        doc_url=doc_url,
        doc_type=doc_type
    )'''
text = re.sub(pattern2, replacement2, text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
