import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_query = """    all_resolutions = UipResolution.query.filter_by(organization_id=org.id).order_by(UipResolution.created_at.desc()).all()"""

new_query = """    is_secretary = current_appointment and current_appointment.position == 'Secretary'
    if is_secretary:
        all_resolutions = UipResolution.query.filter_by(organization_id=org.id).order_by(UipResolution.created_at.desc()).all()
    else:
        all_resolutions = UipResolution.query.filter(
            UipResolution.organization_id == org.id,
            UipResolution.status != 'DRAFT'
        ).order_by(UipResolution.created_at.desc()).all()"""

text = text.replace(old_query, new_query)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated all_resolutions query")
