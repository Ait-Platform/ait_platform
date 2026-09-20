html = """{% extends "program_uip/base.html" %}

{% block content %}
<div class="row mb-4 align-items-center">
    <div class="col-md-8">
        <h1 class="h3 mb-1 text-gray-800">{{ role_title }} Dashboard</h1>
        <p class="text-muted mb-0">Welcome back, <strong>{{ current_user.name or current_user.email }}</strong>. {{ role_desc }}</p>
    </div>
</div>

<div class="row">
    <!-- Governance -->
    <div class="col-xl-3 col-md-6 mb-4">
        <div class="card border-left-primary shadow h-100 py-2">
            <div class="card-body">
                <div class="row no-gutters align-items-center">
                    <div class="col mr-2">
                        <div class="text-xs font-weight-bold text-primary text-uppercase mb-1">
                            Governance</div>
                        <div class="h5 mb-0 font-weight-bold text-gray-800">Voting & Mandates</div>
                        <p class="mt-2 text-sm text-muted">Use the <b>Secretary Tools</b> in your sidebar to cast your vote on active resolutions.</p>
                    </div>
                    <div class="col-auto">
                        <i class="fas fa-gavel fa-2x text-gray-300"></i>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <!-- Secretary Oversight -->
    <div class="col-xl-3 col-md-6 mb-4">
        <div class="card border-left-success shadow h-100 py-2">
            <div class="card-body">
                <div class="row no-gutters align-items-center">
                    <div class="col mr-2">
                        <div class="text-xs font-weight-bold text-success text-uppercase mb-1">
                            Secretary Oversight</div>
                        <div class="h5 mb-0 font-weight-bold text-gray-800">Organogram</div>
                        <p class="mt-2 text-sm text-muted">Read-only view of verified members and empty seats.</p>
                    </div>
                    <div class="col-auto">
                        <i class="fas fa-sitemap fa-2x text-gray-300"></i>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <!-- Financial Health -->
    <div class="col-xl-3 col-md-6 mb-4">
        <div class="card border-left-warning shadow h-100 py-2">
            <div class="card-body">
                <div class="row no-gutters align-items-center">
                    <div class="col mr-2">
                        <div class="text-xs font-weight-bold text-warning text-uppercase mb-1">
                            Financial Health</div>
                        <div class="h5 mb-0 font-weight-bold text-gray-800">Budget Status</div>
                        <p class="mt-2 text-sm text-muted">Read-only view of precinct finances and commitments.</p>
                    </div>
                    <div class="col-auto">
                        <i class="fas fa-coins fa-2x text-gray-300"></i>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Ground Operations -->
    <div class="col-xl-3 col-md-6 mb-4">
        <div class="card border-left-info shadow h-100 py-2">
            <div class="card-body">
                <div class="row no-gutters align-items-center">
                    <div class="col mr-2">
                        <div class="text-xs font-weight-bold text-info text-uppercase mb-1">
                            Ground Operations</div>
                        <div class="h5 mb-0 font-weight-bold text-gray-800">Service Requests</div>
                        <p class="mt-2 text-sm text-muted">Read-only view of open work orders and SLA clocks.</p>
                    </div>
                    <div class="col-auto">
                        <i class="fas fa-hard-hat fa-2x text-gray-300"></i>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>

<div class="row mt-4">
    <div class="col-12">
        <div class="alert alert-info">
            <i class="fas fa-info-circle mr-2"></i> <strong>Notice:</strong> Your detailed oversight modules are being populated. In the meantime, please use the <b>Secretary Tools</b> link in your left-hand sidebar to access the Digital Voting Room.
        </div>
    </div>
</div>
{% endblock %}"""

with open("templates/program_uip/dashboards/placeholder_workspace.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Updated dashboard template to the true Overseer blueprint.")
