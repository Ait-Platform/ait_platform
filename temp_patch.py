import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_verify = """@uip_bp.route("/<org_slug>/verify/committee", methods=["GET", "POST"])
@login_required
def verify_committee(org_slug):
    org = g.organization
    
    from flask import request, redirect, url_for, flash
    from flask_login import current_user
    from sqlalchemy import func
    from sqlalchemy.exc import ProgrammingError
    from app.models.uip_governance import UipCommitteeTerm, UipCommitteeMember
    from app import db
    from app.models.core import CoreInteraction
    
    try:
        term = UipCommitteeTerm.query.filter_by(organization_id=org.id).first()
        if not term:
            # Also log a claim for post-founding members so the Chair can review them
            claim = CoreInteraction.query.filter_by(
                organization_id=org.id,
                creator_id=current_user.id,
                interaction_type="committee_claim",
                status="OPEN"
            ).first()
            if request.method == "GET":
                return render_template("program_uip/claim_committee.html", org=org)
                
            if not claim:
                level = request.form.get("level", "Unknown Level")
                position = request.form.get("position", "Committee Member")
                portfolio = request.form.get("portfolio", "")"""

# wait, the file might differ slightly. Let me fetch the exact block first.
