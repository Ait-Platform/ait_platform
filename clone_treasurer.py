# Clone Workspace
with open("templates/program_uip/dashboards/chairman_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()
text = text.replace("Chairman Dashboard", "Treasurer Dashboard")
text = text.replace("uip_bp.chairman_voting_room", "uip_bp.treasurer_voting_room")
with open("templates/program_uip/dashboards/treasurer_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)

# Clone Voting Room
with open("templates/program_uip/dashboards/chairman_voting_room.html", "r", encoding="utf-8") as f:
    text = f.read()
text = text.replace("uip_bp.chairman_workspace", "uip_bp.treasurer_workspace")
text = text.replace("uip_bp.chairman_view_resolution", "uip_bp.treasurer_view_resolution")
text = text.replace("Back to Chairman Dashboard", "Back to Treasurer Dashboard")
with open("templates/program_uip/dashboards/treasurer_voting_room.html", "w", encoding="utf-8") as f:
    f.write(text)

# Clone Resolution View
with open("templates/program_uip/dashboards/chairman_resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()
text = text.replace("uip_bp.chairman_voting_room", "uip_bp.treasurer_voting_room")
text = text.replace("uip_bp.chairman_vote_resolution", "uip_bp.treasurer_vote_resolution")
with open("templates/program_uip/dashboards/treasurer_resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Cloned HTML templates for treasurer")
