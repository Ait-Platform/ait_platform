import re

with open("artifacts/rcm-stages123-release/app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Modify verify_staff to handle GET and POST
old_verify = """@uip_bp.route("/<org_slug>/verify/staff", methods=["GET"])
@login_required
def verify_staff(org_slug):"""

new_verify = """@uip_bp.route("/<org_slug>/verify/staff", methods=["GET", "POST"])
@login_required
def verify_staff(org_slug):"""

text = text.replace(old_verify, new_verify)

# Modify the logic inside verify_staff where it creates the claim
old_claim = """        claim = CoreInteraction(
            organization_id=org.id,
            creator_id=current_user.id,
            interaction_type="staff_claim",
            title="Staff Membership Claim",
            description=f"User {current_user.email} claims to be a staff member.",
            status="OPEN"
        )"""

new_claim = """        if request.method == "GET":
            return render_template("program_uip/claim_staff.html", org=org)
            
        role = request.form.get("role", "Staff Member")
        company = request.form.get("company", "")
        
        title = f"Operations Claim - {role}"
        desc = f"User {current_user.email} claims to be {role}."
        if company:
            title += f" ({company})"
            desc += f" Company: {company}."
            
        claim = CoreInteraction(
            organization_id=org.id,
            creator_id=current_user.id,
            interaction_type="staff_claim",
            title=title,
            description=desc,
            status="OPEN"
        )"""

text = text.replace(old_claim, new_claim)

with open("artifacts/rcm-stages123-release/app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated verify_staff in routes.py")
