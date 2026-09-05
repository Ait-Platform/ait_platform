import re

with open('app/uip/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_routes = """

@uip_bp.route("/<org_slug>/settings", methods=["GET", "POST"])
@login_required
def org_settings(org_slug):
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    
    # Check permissions (only Manager or Committee Member can edit)
    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
    if not assignment or assignment.role.slug not in ["manager", "committee_member", "owner"]:
        flash("You do not have permission to access organisation settings.", "danger")
        return redirect(url_for("uip_bp.dashboard", org_slug=org_slug))
        
    if request.method == "POST":
        org.name = request.form.get("name")
        org.area = request.form.get("area")
        org.municipality_ref = request.form.get("municipality_ref")
        org.contact_email = request.form.get("contact_email")
        org.contact_phone = request.form.get("contact_phone")
        org.status = request.form.get("status")
        db.session.commit()
        flash("Organisation settings updated successfully.", "success")
        return redirect(url_for("uip_bp.org_settings", org_slug=org_slug))
        
    return render_template("uip/admin/settings.html", org=org)
"""

if "def org_settings" not in text:
    with open('app/uip/routes.py', 'a', encoding='utf-8') as f:
        f.write(new_routes)
    print("Added org_settings route.")
