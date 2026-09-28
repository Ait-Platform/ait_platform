import re

filepath = 'app/program_uip/completion_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Relax header validation
search_val = '''            if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)) or set(reader.fieldnames) != set(CSV_COLUMNS[kind]):
                abort(400, description="Use the exact displayed CSV columns, without duplicates.")'''

replace_val = '''            if not reader.fieldnames:
                abort(400, description="CSV file is empty or missing headers.")'''

content = content.replace(search_val, replace_val)

# 2. Skip signature validation if preview is disabled
search_sig = '''        if commit:
            try:
                if signer.loads(request.form.get("preview_token", ""), max_age=1800) != identity:
                    abort(400, description="Upload the same file and import type that you previewed.")
            except BadSignature:
                abort(400, description="Preview expired or invalid. Preview the file again.")'''

replace_sig = '''        # We are skipping preview token validation for single-step upload'''

content = content.replace(search_sig, replace_sig)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated route logic to allow 1-step save and relaxed columns")
