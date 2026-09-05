import re

with open('app/uip/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace manual org fetch with g.organization
text = re.sub(
    r'    org = CoreOrganization\.query\.filter_by\(slug=org_slug\)\.first_or_404\(\)',
    '    from flask import g\n    org = g.organization',
    text
)

# Remove unused imports if any, but it's fine
with open('app/uip/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
