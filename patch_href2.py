for template in ["secretary_workspace.html", "chairman_workspace.html", "treasurer_workspace.html"]:
    path = f"templates/program_uip/dashboards/{template}"
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    
    html = html.replace("url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=first_tabled_res.id)", "url_for('uip_bp.ratification_desk', org_slug=org.slug, res_id=first_tabled_res.id)")
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
print("Updated red tile href")
