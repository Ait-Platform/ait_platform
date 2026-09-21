for template in ["secretary_workspace.html", "chairman_workspace.html", "treasurer_workspace.html"]:
    path = f"templates/program_uip/dashboards/{template}"
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    
    # 1. Re-apply Tile 5 link change
    html = html.replace("url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=first_tabled_res.id)", "url_for('uip_bp.ratification_desk', org_slug=org.slug, res_id=first_tabled_res.id)")
    
    # 2. Safely apply Tile 2 counts!
    old_p1 = "{{ tabled_res }} TABLED - Needs Ratification."
    new_p1 = "{{ proposed_res }} Active | {{ tabled_res }} Tabled"
    html = html.replace(old_p1, new_p1)
    
    old_p2 = "{{ proposed_res }} waiting for committee votes."
    new_p2 = "{{ proposed_res }} Active | 0 Tabled"
    html = html.replace(old_p2, new_p2)
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
print("Safely patched Tile 2 and Tile 5")
