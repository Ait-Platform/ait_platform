import sys

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    c = f.read()

new_route = """
@uip_bp.route("/<org_slug>/provider/update", methods=["POST"])
@login_required
def update_provider_card(org_slug):
    org = g.organization
    from app.models.uip import UipProviderAccount, UipProvider
    account = UipProviderAccount.query.filter_by(user_id=current_user.id).join(UipProvider).filter(UipProvider.organization_id == org.id).first()
    if not account:
        abort(403)
    provider = UipProvider.query.get(account.provider_id)
    
    name = request.form.get("name", "").strip()
    if name:
        provider.name = name
    provider.service_type = request.form.get("service_type", provider.service_type)
    provider.contact_email = request.form.get("contact_email", provider.contact_email)
    provider.contact_phone = request.form.get("contact_phone", provider.contact_phone)
    
    db.session.commit()
    flash("Provider Card updated.", "success")
    return redirect(url_for("uip_bp.provider_dashboard", org_slug=org_slug))

"""

c = c.replace(
"""@uip_bp.route("/<org_slug>/provider/create", methods=["POST"])""",
new_route + """@uip_bp.route("/<org_slug>/provider/create", methods=["POST"])"""
)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(c)
