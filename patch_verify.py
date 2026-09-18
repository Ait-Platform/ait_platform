import re

with open("artifacts/rcm-stages123-release/app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Modify verify_committee to handle GET and POST
old_verify = """@uip_bp.route("/<org_slug>/verify/committee", methods=["GET"])
@login_required
def verify_committee(org_slug):"""

new_verify = """@uip_bp.route("/<org_slug>/verify/committee", methods=["GET", "POST"])
@login_required
def verify_committee(org_slug):"""

text = text.replace(old_verify, new_verify)

# Modify the logic inside verify_committee where it creates the claim
old_claim = """            if not claim:
                claim = CoreInteraction(
                    organization_id=org.id,
                    creator_id=current_user.id,
                    interaction_type="committee_claim",
                    title="Committee Membership Claim",
                    description=f"User {current_user.email} claims to be a committee member.",
                    status="OPEN"
                )"""

new_claim = """            if request.method == "GET":
                return render_template("program_uip/claim_committee.html", org=org)
                
            if not claim:
                level = request.form.get("level", "Unknown Level")
                position = request.form.get("position", "Committee Member")
                portfolio = request.form.get("portfolio", "")
                
                title = f"{level} Claim - {position}"
                desc = f"User {current_user.email} claims to be {position} on the {level}."
                if portfolio:
                    title += f" ({portfolio})"
                    desc += f" Portfolio: {portfolio}."
                    
                claim = CoreInteraction(
                    organization_id=org.id,
                    creator_id=current_user.id,
                    interaction_type="committee_claim",
                    title=title,
                    description=desc,
                    status="OPEN"
                )"""

text = text.replace(old_claim, new_claim)

# Add the verify_unknown route
unknown_route = """
@uip_bp.route("/<org_slug>/verify/unknown", methods=["GET"])
@login_required
def verify_unknown(org_slug):
    org = g.organization
    from app.models.core import CoreInteraction
    from app import db
    from flask_login import current_user
    
    claim = CoreInteraction.query.filter_by(
        organization_id=org.id,
        creator_id=current_user.id,
        interaction_type="unknown_claim",
        status="OPEN"
    ).first()
    if not claim:
        claim = CoreInteraction(
            organization_id=org.id,
            creator_id=current_user.id,
            interaction_type="unknown_claim",
            title="Unknown Role Claim",
            description=f"User {current_user.email} is unsure of their role and requests manual triage.",
            status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
    return redirect(url_for("uip_bp.my_access", org_slug=org.slug, claim="unknown_claim"))
"""

if "def verify_unknown" not in text:
    text = text + unknown_route

with open("artifacts/rcm-stages123-release/app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated routes.py")
