html_content = """{% extends "program_uip/base.html" %}
{% block title %}Ratepayer Dashboard - {{ org.name }}{% endblock %}

{% block content %}
<div class="max-w-7xl mx-auto px-4 py-8">
    
    <div class="mb-12 border-b border-slate-200 pb-6">
        <h1 class="text-4xl font-extrabold text-slate-900 tracking-tight">Ratepayer Dashboard</h1>
        <p class="text-slate-500 mt-2 font-medium">Community Member Portal</p>
        <p class="text-indigo-600 mt-1 font-bold text-sm">{{ current_user.name }}</p>
    </div>

    <!-- The 4-Pillar Grid -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
        
        <!-- Pillar 1: Public Voting Room -->
        <a href="#" class="group block relative overflow-hidden rounded-2xl border bg-white border-slate-200 hover:border-indigo-300 hover:shadow-md transition-all duration-300">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-xl flex items-center justify-center text-2xl bg-indigo-50 text-indigo-600 group-hover:bg-indigo-600 group-hover:text-white transition-colors">
                        <i class="fas fa-vote-yea"></i>
                    </div>
                    {% if public_resolutions|length > 0 %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-indigo-100 text-indigo-700">
                        {{ public_resolutions|length }} Active
                    </span>
                    {% endif %}
                </div>
                <h3 class="text-xl font-bold text-slate-900 mb-2 group-hover:text-indigo-700 transition-colors">Public Voting Room</h3>
                <p class="text-sm text-slate-500 leading-relaxed">Participate in community-wide mandates, budget approvals, and general meetings.</p>
            </div>
            <div class="bg-slate-50 px-8 py-3 text-xs font-bold text-slate-500 uppercase tracking-widest border-t border-slate-100 group-hover:bg-indigo-50 group-hover:text-indigo-700 transition-colors flex justify-between items-center">
                <span>Enter Room</span>
                <i class="fas fa-arrow-right"></i>
            </div>
        </a>

        <!-- Pillar 2: Service Requests -->
        <a href="#" class="group block relative overflow-hidden rounded-2xl border bg-white border-slate-200 hover:border-amber-300 hover:shadow-md transition-all duration-300">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-xl flex items-center justify-center text-2xl bg-amber-50 text-amber-600 group-hover:bg-amber-500 group-hover:text-white transition-colors">
                        <i class="fas fa-hard-hat"></i>
                    </div>
                    {% if my_requests|length > 0 %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-amber-100 text-amber-700">
                        {{ my_requests|length }} Logged
                    </span>
                    {% endif %}
                </div>
                <h3 class="text-xl font-bold text-slate-900 mb-2 group-hover:text-amber-700 transition-colors">My Service Requests</h3>
                <p class="text-sm text-slate-500 leading-relaxed">Log municipal faults, track progress, and view automated updates from the precinct manager.</p>
            </div>
            <div class="bg-slate-50 px-8 py-3 text-xs font-bold text-slate-500 uppercase tracking-widest border-t border-slate-100 group-hover:bg-amber-50 group-hover:text-amber-700 transition-colors flex justify-between items-center">
                <span>View Tracker</span>
                <i class="fas fa-arrow-right"></i>
            </div>
        </a>

        <!-- Pillar 3: Step Up & Volunteer -->
        <a href="{{ url_for('uip_bp.public_organogram', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-white border-slate-200 hover:border-emerald-300 hover:shadow-md transition-all duration-300">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-xl flex items-center justify-center text-2xl bg-emerald-50 text-emerald-600 group-hover:bg-emerald-600 group-hover:text-white transition-colors">
                        <i class="fas fa-users"></i>
                    </div>
                </div>
                <h3 class="text-xl font-bold text-slate-900 mb-2 group-hover:text-emerald-700 transition-colors">Step Up & Volunteer</h3>
                <p class="text-sm text-slate-500 leading-relaxed">View the live community organogram. Spot an empty seat like 'Block Captain' and claim it.</p>
            </div>
            <div class="bg-slate-50 px-8 py-3 text-xs font-bold text-slate-500 uppercase tracking-widest border-t border-slate-100 group-hover:bg-emerald-50 group-hover:text-emerald-700 transition-colors flex justify-between items-center">
                <span>View Organogram</span>
                <i class="fas fa-arrow-right"></i>
            </div>
        </a>

        <!-- Pillar 4: Property Profile -->
        <a href="{{ url_for('uip_bp.verify_ratepayer', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-white border-slate-200 hover:border-slate-400 hover:shadow-md transition-all duration-300">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-xl flex items-center justify-center text-2xl bg-slate-100 text-slate-600 group-hover:bg-slate-800 group-hover:text-white transition-colors">
                        <i class="fas fa-home"></i>
                    </div>
                    {% if my_properties|length > 0 %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-emerald-100 text-emerald-700">
                        <i class="fas fa-check-circle mr-1"></i> Verified
                    </span>
                    {% else %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-amber-100 text-amber-700">
                        Pending
                    </span>
                    {% endif %}
                </div>
                <h3 class="text-xl font-bold text-slate-900 mb-2 group-hover:text-slate-800 transition-colors">My Property Profile</h3>
                <p class="text-sm text-slate-500 leading-relaxed">Manage your registered properties, update contact details, and view your verification status.</p>
            </div>
            <div class="bg-slate-50 px-8 py-3 text-xs font-bold text-slate-500 uppercase tracking-widest border-t border-slate-100 group-hover:bg-slate-200 group-hover:text-slate-800 transition-colors flex justify-between items-center">
                <span>Manage Profile</span>
                <i class="fas fa-arrow-right"></i>
            </div>
        </a>

    </div>

</div>
{% endblock %}
"""

with open("templates/program_uip/dashboards/ratepayer_workspace.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print("Created ratepayer_workspace.html template")
