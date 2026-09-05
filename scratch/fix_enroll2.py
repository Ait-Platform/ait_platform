import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_code = """@sace_bp.route('/sace/enroll/<activity_slug>', methods=['GET', 'POST'])
def enroll(activity_slug):
    from flask import request, session, redirect, url_for, render_template, g
    from app.payments.pricing import price_for_country
    
    subject = f"sace_{activity_slug}"
    country = getattr(g, "country_iso2", "ZA")
    price_info = price_for_country(country, subject)"""

new_code = """@sace_bp.route('/sace/enroll/<activity_slug>', methods=['GET', 'POST'])
def enroll(activity_slug):
    from flask import request, session, redirect, url_for, render_template, g
    from app.payments.pricing import price_for_country
    from app.models.auth import AuthSubject
    
    subject = f"sace_{activity_slug}"
    subject_obj = AuthSubject.query.filter_by(slug=subject).first()
    country = getattr(g, "country_iso2", "ZA")
    price_info = price_for_country(subject_obj.id if subject_obj else 0, country)"""

content = content.replace(old_code, new_code)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed enroll route pricing call")
