import re

routes_path = 'app/uip/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

new_routes = '''@uip_bp.route("/")
def uip_start():
    return render_template('uip/public_about.html')

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

    quote = get_quote_for_subject_country(subject.slug, country_code)
    session["country_code"] = country_code

    return render_template(
        "uip/price.html",
        subject=subject,
        country_code=country_code,
        quote=quote
    )'''

pattern = r'@uip_bp\.route\("/"\).*?quote=quote\n    \)'
text = re.sub(pattern, new_routes, text, flags=re.DOTALL)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
