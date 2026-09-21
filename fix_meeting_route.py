with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

route = """
@uip_bp.route("/<org_slug>/fix-meeting")
@login_required
def fix_meeting(org_slug):
    from app.models.uip import UipCommitteeMeeting
    import datetime
    org = g.organization
    meetings = UipCommitteeMeeting.query.filter_by(organization_id=org.id).all()
    count = 0
    for m in meetings:
        if m.status == "CONCLUDED" and "AGM" in m.title:
            m.status = "SCHEDULED"
            m.scheduled_at = datetime.datetime(2026, 10, 1, 18, 0)
            count += 1
    db.session.commit()
    flash(f"Fixed {count} meetings. They are now SCHEDULED for the future.", "success")
    return redirect(url_for('uip_bp.meeting_list', org_slug=org.slug))
"""

text += "\n" + route
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added fix_meeting route")
