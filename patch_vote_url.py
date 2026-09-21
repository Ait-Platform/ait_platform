with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id)", "vote_url|default(url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id))")

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)

with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    c_text = f.read()

c_text = c_text.replace("back_text='Back to Voting & Mandates'", "back_text='Back to Voting & Mandates',\n        vote_url=url_for('uip_bp.chairman_vote_resolution', org_slug=org.slug, res_id=res.id)")
c_text = c_text.replace("back_text='Back to Voting & Mandates',\n        vote_url=url_for('uip_bp.chairman_vote_resolution', org_slug=org.slug, res_id=res.id)", "back_text='Back to Voting & Mandates',\n        vote_url=url_for('uip_bp.treasurer_vote_resolution', org_slug=org.slug, res_id=res.id)", 1) # Note: this is a bit hacky because I replaced both with the same string. Let me fix it properly.

# Let's just do a reliable replace:
c_text = c_text.replace("back_url=url_for('uip_bp.chairman_voting_room', org_slug=org.slug),\n        back_text='Back to Voting & Mandates'", "back_url=url_for('uip_bp.chairman_voting_room', org_slug=org.slug),\n        back_text='Back to Voting & Mandates',\n        vote_url=url_for('uip_bp.chairman_vote_resolution', org_slug=org.slug, res_id=res.id)")
c_text = c_text.replace("back_url=url_for('uip_bp.treasurer_voting_room', org_slug=org.slug),\n        back_text='Back to Voting & Mandates'", "back_url=url_for('uip_bp.treasurer_voting_room', org_slug=org.slug),\n        back_text='Back to Voting & Mandates',\n        vote_url=url_for('uip_bp.treasurer_vote_resolution', org_slug=org.slug, res_id=res.id)")

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(c_text)

print("Updated vote_url in universal template")
