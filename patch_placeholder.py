with open("templates/program_uip/dashboards/placeholder_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We will change the URL in the Governance tile to go to chairman_voting_room
text = text.replace("Use the <b>Secretary Tools</b> in your sidebar to cast your vote on active resolutions.", 
                    "Click here to enter the dedicated Chairman Voting Room and cast your votes.")

# We need to wrap the whole tile in an a tag or add a button.
text = text.replace("""<div class="col-auto">
                        <i class="fas fa-gavel fa-2x text-gray-300"></i>
                    </div>""", 
                    """<div class="col-auto">
                        <i class="fas fa-gavel fa-2x text-gray-300"></i>
                    </div>
                </div>
                <div class="mt-3">
                    <a href="{{ url_for('uip_bp.chairman_voting_room', org_slug=org.slug) }}" class="btn btn-primary btn-sm btn-block">Enter Voting Room</a>
                </div>""")

with open("templates/program_uip/dashboards/placeholder_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated placeholder workspace with Chairman Voting Room link")
