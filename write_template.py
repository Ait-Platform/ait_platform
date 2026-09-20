html = """{% extends "program_uip/layout.html" %}

{% block content %}
<div class="row mb-4 align-items-center">
    <div class="col-md-8">
        <h1 class="h3 mb-1 text-gray-800">{{ role_title }} Workspace</h1>
        <p class="text-muted mb-0">{{ role_desc }}</p>
    </div>
</div>

<div class="row">
    <div class="col-12">
        <div class="card shadow mb-4 border-left-primary">
            <div class="card-body py-5 text-center">
                <i class="fas fa-tools fa-3x text-gray-300 mb-3"></i>
                <h4 class="mb-3">Workspace Under Construction</h4>
                <p class="text-muted mb-4 mx-auto" style="max-width: 600px;">
                    The custom dashboard widgets for the {{ role_title }} are currently being built to provide you with the most relevant data.
                </p>
                <div class="alert alert-info d-inline-block px-4 py-3 text-left">
                    <strong><i class="fas fa-info-circle mr-1"></i> Interim Command Centre</strong><br>
                    While your dedicated dashboard is being completed, you have been granted access to the shared Secretary Switchboard.
                    <br><br>
                    Please click <b>"Switchboard"</b> under the <b>Secretary Tools</b> section in the left-hand sidebar to access the Digital Voting Room and active resolutions.
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}"""

with open("templates/program_uip/dashboards/placeholder_workspace.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Wrote placeholder template")
