with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Add redirect for secretary inside committee_dashboard
old_check = """    if not current_appointment and not is_manager:
        abort(403)"""

new_check = """    if not current_appointment and not is_manager:
        abort(403)
        
    if current_appointment and current_appointment.position.lower() == "secretary":
        return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))"""

text = text.replace(old_check, new_check)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
