import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''            if request.args.get("view") != "register":
                if pos == "secretary":
                    return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
                elif pos in ["chairperson", "chairman"]:
                    return redirect(url_for("uip_bp.chairman_workspace", org_slug=org.slug))
                elif pos in ["vice-chairperson", "vice chairman"]:
                    return redirect(url_for("uip_bp.vice_chair_workspace", org_slug=org.slug))
                elif pos == "treasurer":
                    return redirect(url_for("uip_bp.treasurer_workspace", org_slug=org.slug))'''

replace_code = '''            if request.args.get("view") != "register":
                if pos == "secretary":
                    return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
                elif pos in ["chairperson", "chairman"]:
                    return redirect(url_for("uip_bp.chairman_workspace", org_slug=org.slug))
                elif pos in ["vice-chairperson", "vice chairman"]:
                    return redirect(url_for("uip_bp.vice_chair_workspace", org_slug=org.slug))
                elif pos == "treasurer":
                    return redirect(url_for("uip_bp.treasurer_workspace", org_slug=org.slug))
                elif "municipal" in pos:
                    return redirect(url_for("uip_bp.mo_dashboard", org_slug=org.slug))'''

content = content.replace(search_code, replace_code)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated committee_dashboard redirect for MO")
