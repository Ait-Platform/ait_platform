with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_func = """def _require_secretary():
    org = g.organization
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or current_appointment.position != "Secretary":
        abort(403, description="Access restricted to the active Secretary.")
    return current_appointment"""

new_func = """def _require_secretary():
    org = g.organization
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or not current_appointment.position or current_appointment.position.strip().lower() != "secretary":
        abort(403, description="Access restricted to the active Secretary.")
    return current_appointment"""

text = text.replace(old_func, new_func)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched _require_secretary")
