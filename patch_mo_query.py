import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_query = """    escalations = UipMunicipalReferral.query.filter_by(
        organization_id=org.id,
        status="ESCALATED_TO_MO"
    ).all()"""

new_query = """    escalations = UipMunicipalReferral.query.filter(
        UipMunicipalReferral.organization_id == org.id,
        UipMunicipalReferral.status.in_(["ESCALATED_TO_MO", "ACKNOWLEDGED", "DISPATCHED"])
    ).all()"""

if old_query in text:
    text = text.replace(old_query, new_query)
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Updated MO query successfully!")
else:
    print("Could not find old query")
