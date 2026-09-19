with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_route = """            if pos in ["chairman", "vice chairman", "chair", "chairperson", "vice chair"]:
                from app.program_uip.presentation import executive
                return render_template("program_uip/dashboards/manager.html", org=org, overview=executive(org.id, current_user.id))"""

new_route = """            if pos in ["chairman", "vice-chairperson", "vice chairman", "chair", "chairperson", "vice chair"]:
                return redirect(url_for("uip_bp.exco_workspace", org_slug=org_slug))"""

if old_route in text:
    text = text.replace(old_route, new_route)
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Updated Chair dashboard route")
else:
    print("Could not find old Chair route")
