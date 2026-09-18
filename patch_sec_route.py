import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_route = """    # 2. Fetch pending access resolutions
    proposed_resolutions = UipResolution.query.filter(
        UipResolution.organization_id == org.id,
        UipResolution.status == "PROPOSED"
    ).filter(
        UipResolution.title.like("%Access Resolution%")
    ).order_by(UipResolution.created_at.desc()).all()
    
    return render_template(
        "program_uip/dashboards/secretary_workspace.html",
        org=org,
        open_claims=enriched_claims,
        proposed_resolutions=proposed_resolutions
    )"""

new_route = """    # 2. Fetch resolution status for the switches
    # We want to know if there are any TABLED resolutions (Urgent/Red)
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    # We want to know if there are any PROPOSED resolutions (Amber/Waiting)
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    
    # 3. Calculate Switch Colors
    switch_gate = "red" if len(enriched_claims) > 0 else "grey"
    
    switch_res = "grey"
    if tabled_res > 0:
        switch_res = "red"
    elif proposed_res > 0:
        switch_res = "amber"
        
    return render_template(
        "program_uip/dashboards/secretary_workspace.html",
        org=org,
        open_claims=enriched_claims,
        tabled_res=tabled_res,
        proposed_res=proposed_res,
        switch_gate=switch_gate,
        switch_res=switch_res
    )"""

# I need to find the old route properly
if old_route in text:
    text = text.replace(old_route, new_route)
    print("Replaced route successfully")
else:
    # Try regex
    text = re.sub(r'    # 2\. Fetch pending access resolutions.*?proposed_resolutions=proposed_resolutions\n    \)', new_route, text, flags=re.DOTALL)
    print("Replaced route using regex")

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
