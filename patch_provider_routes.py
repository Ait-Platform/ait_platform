import sys
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    c = f.read()

new_routes = """
@uip_bp.route("/<org_slug>/provider-dashboard", methods=["GET"])
@login_required
def provider_dashboard(org_slug):
    org = g.organization
    from app.models.uip import UipProviderAccount, UipProvider
    # Check if they have an account
    account = UipProviderAccount.query.filter_by(user_id=current_user.id).join(UipProvider).filter(UipProvider.organization_id == org.id).first()
    
    provider_card = None
    if account:
        provider_card = UipProvider.query.get(account.provider_id)
        
    return render_template("program_uip/dashboards/provider_dashboard.html", org=org, provider=provider_card)

@uip_bp.route("/<org_slug>/provider/create", methods=["POST"])
@login_required
def create_provider_card(org_slug):
    org = g.organization
    from app.models.uip import UipProvider, UipProviderAccount
    name = request.form.get("name", "").strip()
    if not name:
        flash("Provider name is required.", "error")
        return redirect(url_for("uip_bp.provider_dashboard", org_slug=org_slug))
        
    provider = UipProvider(
        organization_id=org.id,
        name=name,
        service_type=request.form.get("service_type", ""),
        contact_email=request.form.get("contact_email", ""),
        contact_phone=request.form.get("contact_phone", "")
    )
    db.session.add(provider)
    db.session.flush()
    
    account = UipProviderAccount(
        provider_id=provider.id,
        user_id=current_user.id,
        is_admin=True
    )
    db.session.add(account)
    db.session.commit()
    flash("Provider Card created.", "success")
    return redirect(url_for("uip_bp.provider_dashboard", org_slug=org_slug))
"""

c = c.replace(
"""def verify_provider(org_slug):
    if providers.has_workspace(g.organization.id, current_user.id):
        return redirect(url_for("uip_bp.work_order_list", org_slug=org_slug))
    from werkzeug.exceptions import Forbidden
    try:
        audit.authorize(g.organization.id, current_user.id, ("provider",))
    except Forbidden:
        return _operational_claim(org_slug, "provider", "Service Provider")
    flash("Provider authority is verified. An active provider association is still required before work-order access.", "info")
    return redirect(url_for("uip_bp.my_access", org_slug=org_slug, claim="provider"))""",
"""def verify_provider(org_slug):
    # Registration grants access to the generic Provider Dashboard.
    # No secretary admission or roles required.
    return redirect(url_for("uip_bp.provider_dashboard", org_slug=org_slug))""" + new_routes
)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(c)
