import re

routes_path = 'app/uip/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

new_routes = '''@uip_bp.route("/")
def uip_start():
    # Force South Africa selection automatically and go to price
    return redirect(url_for('uip_bp.price_page', country='ZA'))

@uip_bp.route("/price")
def price_page():
    from app.models.auth import AuthSubject
    from app.enrollment.logic import get_quote_for_subject_country
    from flask import session

    subject = AuthSubject.query.filter(
        db.func.lower(AuthSubject.slug) == 'uip').first()
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for('public_bp.welcome'))

    country_code = (request.args.get("country") or "").strip().upper()
    if not country_code:
        country_code = 'ZA'  # Default to SA

    # Actually UIP operates strictly in SA, but quote logic needs the standard dictionary
    quote = get_quote_for_subject_country(subject.slug, country_code)
    
    session["country_code"] = country_code

    return render_template(
        "uip/price.html",
        subject=subject,
        country_code=country_code,
        quote=quote
    )
'''

pattern = r'@uip_bp\.route\("/"\)\s*\ndef uip_start\(\):\s*\n\s*# Public marketing and landing page for UIPs\s*\n\s*return render_template\(\'uip/public_about\.html\'\)'

text = re.sub(pattern, new_routes, text)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
