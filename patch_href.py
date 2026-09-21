for template in ["secretary_workspace.html", "chairman_workspace.html", "treasurer_workspace.html"]:
    path = f"templates/program_uip/dashboards/{template}"
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    
    # We want to replace href="{% if first_tabled_res %}{{ url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=first_tabled_res.id) }}{% else %}#{% endif %}"
    # with href="{% if first_tabled_res %}{{ url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=first_tabled_res.id) }}{% else %}{{ url_for('uip_bp.meeting_list', org_slug=org.slug) }}{% endif %}"
    
    html = html.replace("{% else %}#{% endif %}", "{% else %}{{ url_for('uip_bp.meeting_list', org_slug=org.slug) }}{% endif %}")
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
print("Fixed default href for meetings tile")
