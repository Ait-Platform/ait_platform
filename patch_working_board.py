html = """{% extends "program_uip/base.html" %}

{% block content %}
<div class="row mb-4 align-items-center">
    <div class="col-md-8">
        <h1 class="h3 mb-1 text-gray-800">{{ role_title }} Dashboard</h1>
        <p class="text-muted mb-0">Welcome back, <strong>{{ current_user.name or current_user.email }}</strong>. {{ role_desc }}</p>
    </div>
</div>

<div class="row">
    <!-- Quick Access to Secretary Board -->
    <div class="col-xl-6 col-md-6 mb-4">
        <div class="card border-left-primary shadow h-100 py-2">
            <div class="card-body">
                <div class="row no-gutters align-items-center">
                    <div class="col mr-2">
                        <div class="text-xs font-weight-bold text-primary text-uppercase mb-1">
                            Shared ExCo Command Centre</div>
                        <div class="h5 mb-0 font-weight-bold text-gray-800">Secretary Workspace</div>
                        <p class="mt-2 text-sm text-muted mb-3">Access the Digital Voting Room, the Organogram, and active UIP Resolutions here.</p>
                        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="btn btn-primary">
                            Enter Secretary Board <i class="fas fa-arrow-right ml-1"></i>
                        </a>
                    </div>
                    <div class="col-auto">
                        <i class="fas fa-gavel fa-3x text-gray-300"></i>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}"""

with open("templates/program_uip/dashboards/placeholder_workspace.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Updated dashboard template to remove under construction and add direct button.")
