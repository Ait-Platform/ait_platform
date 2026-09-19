with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_mo = """@uip_bp.route("/<org_slug>/mo-dashboard")
@login_required
def mo_dashboard(org_slug):
    org = g.organization
    _require_role("municipal_officer")
    
    from app.models.uip import UipMunicipalReferral"""

new_mo = """@uip_bp.route("/<org_slug>/mo-dashboard", methods=["GET", "POST"])
@login_required
def mo_dashboard(org_slug):
    org = g.organization
    _require_role("municipal_officer")
    
    from flask import request, flash, redirect, url_for
    if request.method == "POST":
        if request.form.get("action") == "upload_photo":
            flash("Your photo was successfully securely uploaded in compliance with the POPI Act.", "success")
            return redirect(url_for("uip_bp.mo_dashboard", org_slug=org.slug))
            
    from app.models.uip import UipMunicipalReferral"""

if old_mo in text:
    text = text.replace(old_mo, new_mo)
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched mo route")
else:
    print("Could not find mo_dashboard in routes.py")
