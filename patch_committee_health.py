html = """{% extends "program_uip/base.html" %}
{% block title %}{{ org.name }} - Committee Dashboard{% endblock %}

{% block content %}
<div class="max-w-6xl mx-auto px-4 mt-8 mb-16">
    <header class="ui-page-head flex justify-between items-start mb-8 border-b border-slate-200 pb-6">
        <div>
            {% if current_appointment and current_appointment.position == "Secretary" %}
            <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-indigo-600 hover:text-indigo-800 mb-3 bg-indigo-50 px-3 py-1 rounded-full transition">
                <i class="fas fa-arrow-left mr-2"></i> Back to Command Switchboard
            </a>
            {% endif %}
            <h1 class="text-3xl font-extrabold text-slate-900 mt-1">{% if current_appointment and current_appointment.position == "Secretary" %}Resolution Register{% else %}Committee Dashboard{% endif %}</h1>
            <p class="text-slate-600 mt-2">Executive oversight and governance operations.</p>
        </div>
        <div class="mt-1">
            <span class="bg-indigo-100 text-indigo-800 text-xs px-3 py-1.5 rounded-full font-bold tracking-wide shadow-sm border border-indigo-200">
                <i class="fas fa-university mr-1"></i> {{ org.name }}
            </span>
        </div>
    </header>

    {% include "partials/flash_messages.html" %}

    <!-- 1. PRECINCT ACTIVATION HEALTH -->
    {% set total_vault = org.member_profiles|length if org.member_profiles else 1 %}
    {% set active_members = org.member_profiles|selectattr("eligibility_status", "equalto", "eligible")|list|length %}
    {% set activation_pct = ((active_members / total_vault) * 100)|int if total_vault > 0 else 0 %}
    {% set quorum_target = 50 %} <!-- Example threshold -->
    
    <div class="mb-10 bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
        <div class="p-6 border-b border-slate-100 bg-slate-50 flex justify-between items-center">
            <h2 class="text-lg font-bold text-slate-800"><i class="fas fa-heartbeat text-rose-500 mr-2"></i> Precinct Activation Health</h2>
            <div class="text-sm font-bold text-slate-500">Target Quorum: {{ quorum_target }}%</div>
        </div>
        <div class="p-6">
            <div class="flex justify-between items-end mb-2">
                <div>
                    <div class="text-3xl font-black {% if activation_pct < quorum_target %}text-rose-600{% else %}text-emerald-600{% endif %}">
                        {{ activation_pct }}% Registered
                    </div>
                    <div class="text-sm text-slate-500 font-medium mt-1">
                        {{ active_members }} out of {{ total_vault }} Vault Properties have claimed their profiles.
                    </div>
                </div>
                {% if activation_pct < quorum_target %}
                <div class="px-4 py-2 bg-rose-50 text-rose-700 rounded-lg border border-rose-200 text-sm font-bold animate-pulse">
                    <i class="fas fa-exclamation-triangle mr-1"></i> Quorum Deficit - AGM Blocked
                </div>
                {% else %}
                <div class="px-4 py-2 bg-emerald-50 text-emerald-700 rounded-lg border border-emerald-200 text-sm font-bold">
                    <i class="fas fa-check-circle mr-1"></i> Quorum Achieved
                </div>
                {% endif %}
            </div>
            <!-- Progress Bar -->
            <div class="w-full bg-slate-100 rounded-full h-4 mt-4 border border-slate-200 overflow-hidden relative">
                <div class="h-4 rounded-full transition-all duration-1000 {% if activation_pct < quorum_target %}bg-gradient-to-r from-rose-400 to-rose-500{% else %}bg-gradient-to-r from-emerald-400 to-emerald-500{% endif %}" style="width: {{ activation_pct }}%"></div>
                <!-- Quorum Marker -->
                <div class="absolute top-0 bottom-0 border-l-2 border-slate-800 z-10" style="left: {{ quorum_target }}%;">
                    <div class="absolute -top-6 -translate-x-1/2 text-[10px] font-black text-slate-800 uppercase tracking-widest bg-white px-1">Quorum</div>
                </div>
            </div>
        </div>
    </div>

    <!-- 2. ESCALATION HIT LIST -->
    {% set escalation_list = org.member_profiles|selectattr("eligibility_status", "equalto", "unverified")|selectattr("invite_wave", "equalto", 3)|list %}
    {% if escalation_list %}
    <div class="mb-10 bg-amber-50 border border-amber-200 rounded-xl shadow-sm overflow-hidden">
        <div class="p-4 border-b border-amber-200 bg-amber-100/50 flex justify-between items-center">
            <h2 class="text-sm font-bold text-amber-900 uppercase tracking-widest"><i class="fas fa-clipboard-list mr-2"></i> ExCo Manual Escalation List</h2>
            <span class="bg-amber-600 text-white text-xs font-bold px-2 py-0.5 rounded-full">{{ escalation_list|length }} Holdouts</span>
        </div>
        <div class="p-0">
            <table class="w-full text-left text-sm">
                <thead class="bg-amber-50 text-amber-700 font-bold border-b border-amber-200">
                    <tr>
                        <th class="p-4">Ratepayer / Property</th>
                        <th class="p-4">Contact</th>
                        <th class="p-4">Action</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-amber-100 text-amber-900">
                    {% for rp in escalation_list %}
                    <tr class="hover:bg-amber-100/30 transition">
                        <td class="p-4 font-bold">{{ rp.name }} <br><span class="text-xs font-normal opacity-70">Ref: {{ rp.reference }}</span></td>
                        <td class="p-4">{{ rp.phone or 'No phone' }} <br><span class="text-xs font-normal opacity-70">{{ rp.email }}</span></td>
                        <td class="p-4">
                            <button class="px-3 py-1 bg-white border border-amber-300 rounded text-amber-700 text-xs font-bold hover:bg-amber-50 shadow-sm">Claim Door-Knock</button>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
    {% endif %}

    <!-- 3. MAIN DASHBOARD CONTENT (Resolutions) -->
    <div class="flex justify-between items-center mb-6">
        <h2 class="text-xl font-bold text-slate-800"><i class="fas fa-file-signature text-indigo-500 mr-2"></i> Resolution Register</h2>
        <a href="{{ url_for('uip_bp.draft_resolution', org_slug=org.slug) }}" class="ui-btn ui-btn-primary shadow-sm">
            <i class="fas fa-plus mr-2"></i> Draft New Resolution
        </a>
    </div>

    <!-- The rest of the committee.html resolution list stays exactly the same -->
    <div class="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
        <table class="w-full text-left text-sm">
            <thead class="bg-slate-50 text-slate-600 font-bold border-b border-slate-200">
                <tr>
                    <th class="p-4">Ref</th>
                    <th class="p-4">Title</th>
                    <th class="p-4">Phase</th>
                    <th class="p-4">Voting Scope</th>
                    <th class="p-4">Actions</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-slate-100">
                {% if org.uip_resolutions %}
                    {% for res in org.uip_resolutions|sort(attribute='created_at', reverse=True) %}
                    <tr class="hover:bg-slate-50 transition">
                        <td class="p-4 font-mono text-slate-500 font-medium">RES-{{ res.id }}</td>
                        <td class="p-4 font-bold text-slate-800">{{ res.title }}</td>
                        <td class="p-4">
                            {% if res.status == 'DRAFT' %}
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-slate-100 text-slate-700 border border-slate-200">DRAFT</span>
                            {% elif res.status == 'PROPOSED' %}
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-amber-50 text-amber-700 border border-amber-200">VOTING ACTIVE</span>
                            {% elif res.status == 'TABLED' %}
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-rose-50 text-rose-700 border border-rose-200 animate-pulse">NEEDS RATIFICATION</span>
                            {% elif res.status == 'ADOPTED' %}
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-emerald-50 text-emerald-700 border border-emerald-200">ADOPTED</span>
                            {% elif res.status == 'REJECTED' %}
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-red-50 text-red-700 border border-red-200">REJECTED</span>
                            {% else %}
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-slate-100 text-slate-700 border border-slate-200">{{ res.status }}</span>
                            {% endif %}
                        </td>
                        <td class="p-4 text-slate-600">
                            {% if res.voting_scope == 'EXCO' %}
                            <i class="fas fa-users-cog mr-1 text-purple-500"></i> Core ExCo
                            {% elif res.voting_scope == 'COMMITTEE_ALL' %}
                            <i class="fas fa-users mr-1 text-indigo-500"></i> Full Committee
                            {% elif res.voting_scope == 'PUBLIC' %}
                            <i class="fas fa-globe mr-1 text-emerald-500"></i> Public / Ratepayers
                            {% else %}
                            {{ res.voting_scope }}
                            {% endif %}
                        </td>
                        <td class="p-4">
                            <a href="{{ url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=res.id) }}" class="text-indigo-600 font-bold hover:text-indigo-800">
                                Open &rarr;
                            </a>
                        </td>
                    </tr>
                    {% endfor %}
                {% else %}
                    <tr>
                        <td colspan="5" class="p-8 text-center text-slate-500 italic">No resolutions have been drafted yet.</td>
                    </tr>
                {% endif %}
            </tbody>
        </table>
    </div>

</div>
{% endblock %}"""

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Updated committee.html with activation health widget")
