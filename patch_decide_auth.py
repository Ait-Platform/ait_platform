import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_decide_auth = """    if not current_appointment or current_appointment.position.lower() not in ["chairman", "chairperson", "chair", "vice chair", "vice chairman"]:
        flash("Only the Chairman or Vice Chairman has the authority to lock down and finalize resolutions.", "danger")"""

new_decide_auth = """    if not current_appointment or current_appointment.position.lower() not in ["chairman", "chairperson", "chair", "vice chair", "vice chairman", "secretary"]:
        flash("Only the Chairman, Vice Chairman, or Secretary has the authority to lock down and finalize resolutions.", "danger")"""

text = text.replace(old_decide_auth, new_decide_auth)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated decide_resolution auth")
