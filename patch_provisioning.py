with open("app/program_uip/provisioning_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Change the date logic
old_date = """        if request.method == "POST":
            scheduled_at_str = request.form.get("meeting_date", "2026-09-01")"""
new_date = """        if request.method == "POST":
            scheduled_at_str = request.form.get("meeting_date", "2026-10-01")"""
text = text.replace(old_date, new_date)

# 2. Change the status
old_meeting = """        meeting = UipCommitteeMeeting(
            organization_id=org.id,
            title="Founding AGM",
            meeting_type="FOUNDING",
            scheduled_at=scheduled_at,
            location=venue,
            status="CONCLUDED"
        )"""
new_meeting = """        meeting = UipCommitteeMeeting(
            organization_id=org.id,
            title="Founding AGM",
            meeting_type="FOUNDING",
            scheduled_at=scheduled_at,
            location=venue,
            status="SCHEDULED"
        )"""
text = text.replace(old_meeting, new_meeting)

with open("app/program_uip/provisioning_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated provisioning_routes.py")
