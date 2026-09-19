with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# I need to pass pending_resolutions=len(proposed_resolutions) or a specific query count.
# I'll calculate it in secretary_workspace
old_render = """    return render_template(
        "program_uip/dashboards/secretary_workspace.html",
        org=org,
        open_claims=enriched_claims,
        proposed_resolutions=proposed_resolutions
    )"""

new_render = """    
    # Calculate pending resolutions (PROPOSED) for the alert badge
    pending_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    
    return render_template(
        "program_uip/dashboards/secretary_workspace.html",
        org=org,
        open_claims=enriched_claims,
        proposed_resolutions=proposed_resolutions,
        pending_resolutions=pending_resolutions
    )"""

if old_render in text:
    text = text.replace(old_render, new_render)
    with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched secretary_routes.py with pending_resolutions")
else:
    print("Could not find old_render")
