with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_check = """    if current_appointment and current_appointment.position.lower() == "secretary":
        return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))"""

new_check = """    if current_appointment and current_appointment.position.lower() == "secretary":
        # Allow them to view the register if they explicitly clicked the tile
        if request.args.get("view") != "register":
            return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))"""

text = text.replace(old_check, new_check)
with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched committee_routes")
