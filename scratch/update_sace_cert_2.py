import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

chunk1 = 'completed_date = completed_at.strftime("%d %B %Y")'
chunk1_new = '''completed_date = completed_at.strftime("%d %B %Y")

    from app.utils.branding import get_logo_data_uri, get_seal_data_uri
    logo_data_uri = get_logo_data_uri()
    seal_data_uri = get_seal_data_uri()'''

if chunk1 in text and 'get_logo_data_uri' not in text:
    text = text.replace(chunk1, chunk1_new)

chunk2 = 'certificate_id=certificate_id,'
chunk2_new = '''certificate_id=certificate_id,
            logo_path=logo_data_uri,
            seal_path=seal_data_uri,'''

# Only replace the last occurrence of chunk2 (which is in _generate_sace_certificate_pdf)
if text.count(chunk2) > 0 and 'logo_path=logo_data_uri' not in text:
    # rsplit to replace just the last one
    parts = text.rsplit(chunk2, 1)
    text = chunk2_new.join(parts)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
