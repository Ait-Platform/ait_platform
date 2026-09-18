import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Using regex to replace the entire verify_committee block
import ast

# Find the start of verify_committee
start_idx = text.find('@uip_bp.route("/<org_slug>/verify/committee"')
if start_idx == -1:
    print("Could not find verify_committee")
    exit(1)

# Find the next route to mark the end
next_route_idx = text.find('@uip_bp.route("/<org_slug>/verify/mo"', start_idx)
if next_route_idx == -1:
    print("Could not find next route")
    exit(1)

old_block = text[start_idx:next_route_idx]

new_block = """@uip_bp.route("/<org_slug>/verify/committee", methods=["GET", "POST"])
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
        
        # Rule 1: One Claim at a Time
        existing_claim = CoreInteraction.query.filter_by(
            organization_id=org.id,
            creator_id=current_user.id,
            interaction_type="committee_claim",
            status="OPEN"
        ).first()
        
        if existing_claim and request.method == "POST":
            flash("You already have a pending request. If you made a mistake, please click the 'Unsure / Other' tile.", "warning")
            return redirect(url_for("uip_bp.router_page", org_slug=org.slug))

        if request.method == "GET":
            # For Subcommittees (or general GETs)
            return render_template("program_uip/claim_committee.html", org=org)
            
        if request.method == "POST" and not existing_claim:
            level = request.form.get("level", "Unknown Level")
            position = request.form.get("position", "Committee Member").strip()
            portfolio = request.form.get("portfolio", "").strip()
            
            # Rule 2: Seat Occupied Fallback
            # Only check for specific singular roles
            singular_roles = ["chairman", "vice chairman", "secretary", "treasurer"]
            if position.lower() in singular_roles:
                occupied = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id,
                    func.lower(UipCommitteeMember.position) == position.lower(),
                    UipCommitteeMember.is_active == True
                ).first()
                if occupied:
                    flash(f"The position of {position} is currently occupied. Please click the 'Unsure / Other' tile and the Secretary will sort it out.", "warning")
                    return redirect(url_for("uip_bp.router_page", org_slug=org.slug))
            
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
            )
            db.session.add(claim)
            db.session.commit()
            
        from app.models.uip import UipCommitteeMeeting
        founding_exists = UipCommitteeMeeting.query.filter_by(
            organization_id=org.id, meeting_type="FOUNDING"
        ).first() is not None
        
        if not founding_exists:
            return redirect(url_for("uip_bp.waiting_lounge", org_slug=org.slug, claim="genesis"))
        else:
            return redirect(url_for("uip_bp.waiting_lounge", org_slug=org.slug, claim="committee_claim"))
            
    except ProgrammingError:
        return redirect(url_for("uip_bp.provisioning", org_slug=org.slug))

"""

text = text.replace(old_block, new_block)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated verify_committee routing logic successfully")
