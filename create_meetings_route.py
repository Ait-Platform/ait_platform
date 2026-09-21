with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

route = """
@uip_bp.route("/<org_slug>/meetings")
@login_required
def meeting_list(org_slug):
    org = g.organization
    
    # Require EXCO or Committee member access
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    
    exco_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not exco_check:
        from flask import flash
        flash("Only active Committee Members can access the Meetings Hub.", "error")
        return redirect(url_for("uip_bp.dashboard", org_slug=org.slug))
        
    from app.models.uip import UipCommitteeMeeting
    meetings = UipCommitteeMeeting.query.filter_by(organization_id=org.id).order_by(UipCommitteeMeeting.scheduled_at.desc()).all()
    
    return render_template("program_uip/dashboards/meeting_list.html", org=org, meetings=meetings)
"""

if "def meeting_list" not in text:
    text += "\n" + route

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Added meeting_list route")
