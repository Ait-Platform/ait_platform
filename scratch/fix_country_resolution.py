import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the resolve_country(request)
old_code = """
    from flask import request, session, redirect, url_for, render_template
    from app.payments.pricing import price_for_country
    from app.utils.country_list import resolve_country
    
    subject = f"sace_{activity_slug}"
    country = resolve_country(request)
"""

new_code = """
    from flask import request, session, redirect, url_for, render_template, g
    from app.payments.pricing import price_for_country
    
    subject = f"sace_{activity_slug}"
    country = getattr(g, "country_iso2", "ZA")
"""

content = content.replace(old_code, new_code)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed country resolution")
