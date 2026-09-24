with open("app/program_uip/provisioning_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Revert the date logic
old_date = """        if request.method == "POST":
            scheduled_at_str = request.form.get("meeting_date", "2026-10-01")"""
new_date = """        if request.method == "POST":
            scheduled_at_str = request.form.get("meeting_date", "2026-09-01")"""
text = text.replace(old_date, new_date)

# 2. Revert the status
old_meeting = """        meeting = UipCommitteeMeeting(
            organization_id=org.id,
            title="Founding AGM",
            meeting_type="FOUNDING",
            scheduled_at=scheduled_at,
            location=venue,
            status="SCHEDULED"
        )"""
new_meeting = """        meeting = UipCommitteeMeeting(
            organization_id=org.id,
            title="Founding AGM",
            meeting_type="FOUNDING",
            scheduled_at=scheduled_at,
            location=venue,
            status="CONCLUDED"
        )"""
text = text.replace(old_meeting, new_meeting)

with open("app/program_uip/provisioning_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Reverted provisioning_routes.py back to original genesis logic")

# 3. Create route to revert the DB
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    rtext = f.read()

route = """
@uip_bp.route("/<org_slug>/revert-meeting")
@login_required
def revert_meeting(org_slug):
    from app.models.uip import UipCommitteeMeeting
    import datetime
    org = g.organization
    meetings = UipCommitteeMeeting.query.filter_by(organization_id=org.id).all()
    count = 0
    for m in meetings:
        if m.status == "SCHEDULED" and "AGM" in m.title:
            m.status = "CONCLUDED"
            m.scheduled_at = datetime.datetime(2026, 9, 1, 18, 0)
            count += 1
    db.session.commit()
    flash(f"Reverted {count} Founding AGMs back to CONCLUDED historical records.", "success")
    return redirect(url_for('uip_bp.meeting_list', org_slug=org.slug))
"""
if "def revert_meeting" not in rtext:
    rtext += "\n" + route
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(rtext)
print("Added revert_meeting route")
