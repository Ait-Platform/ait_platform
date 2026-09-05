import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update redirect in email_certificate
redirect_pattern = r'return redirect\(url_for\("sace_bp\.post_test_results"\)\)'
text = re.sub(redirect_pattern, 'return redirect(url_for("reading_bp.subject_home"))', text)

# 2. Add logo/seal to _generate_sace_certificate_pdf
generate_pattern = r'def _generate_sace_certificate_pdf\(.*?def '

def replacement(match):
    chunk = match.group(0)
    # Add branding imports
    if 'get_logo_data_uri' not in chunk:
        chunk = chunk.replace('completed_date = completed_at.strftime("%d %B %Y")', 
            'completed_date = completed_at.strftime("%d %B %Y")\n\n    from app.utils.branding import get_logo_data_uri, get_seal_data_uri\n    logo_data_uri = get_logo_data_uri()\n    seal_data_uri = get_seal_data_uri()')
        
        # Add to render_template
        chunk = chunk.replace('certificate_id=certificate_id,', 'certificate_id=certificate_id,\n            logo_path=logo_data_uri,\n            seal_path=seal_data_uri,')
    return chunk

text = re.sub(generate_pattern, replacement, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

