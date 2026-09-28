import sys

with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    c = f.read()

new_route = """
@uip_bp.route("/<org_slug>/secretary/decline-claim/<int:claim_id>", methods=["POST"])
@login_required
def decline_claim(org_slug, claim_id):
    org = g.organization
    _require_secretary()
    
    from app.models.core import CoreInteraction
    from datetime import datetime, timezone
    
    claim = CoreInteraction.query.filter_by(
        id=claim_id,
        organization_id=org.id,
        status="OPEN"
    ).first_or_404()
    
    claim.status = "DECLINED"
    claim.closed_by = current_user.id
    claim.closed_at = datetime.now(timezone.utc)
    
    db.session.commit()
    flash("Access claim was removed/declined.", "success")
    return redirect(url_for("uip_bp.secretary_intake", org_slug=org_slug))

"""

c = c.replace(
"""@uip_bp.route("/<org_slug>/secretary-intake")""",
new_route + """@uip_bp.route("/<org_slug>/secretary-intake")"""
)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(c)
