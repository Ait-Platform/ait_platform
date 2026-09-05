import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Make catalog public
content = content.replace("@sace_bp.route('/sace/catalog')\n@login_required\ndef catalog():", "@sace_bp.route('/sace/catalog')\ndef catalog():")

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Made catalog public")
