import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_code = """
    from flask import request, session, redirect, url_for, render_template, g
    from app.payments.pricing import price_for_country
    
    subject = f"sace_{activity_slug}"
    country = getattr(g, "country_iso2", "ZA")
    price_info = price_for_country(country, subject)
"""

new_code = """
    from flask import request, session, redirect, url_for, render_template, g
    from app.payments.pricing import price_for_country
    from app.models.auth import AuthSubject
    
    subject_slug = f"sace_{activity_slug}"
    subject_obj = AuthSubject.query.filter_by(slug=subject_slug).first()
    
    country = getattr(g, "country_iso2", "ZA")
    price_info = price_for_country(subject_obj.id if subject_obj else 0, country)
"""

# wait, there's another occurrence of subject in the function that I shouldn't break
# let's look at the original code in routes.py
