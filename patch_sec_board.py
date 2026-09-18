html = """{% extends "program_uip/base_public.html" %}
{% block title %}Command Switchboard - {{ org.name }}{% endblock %}

{% block content %}
<div class="max-w-6xl mx-auto px-4 py-8">
    <div class="mb-12 border-b border-slate-200 pb-6 flex justify-between items-end">
        <div>
            <h1 class="text-4xl font-extrabold text-slate-900 tracking-tight">Secretary Command</h1>
            <p class="text-slate-500 mt-2 font-medium">Alert-Driven Operations Dashboard</p>
        </div>
        <div class="text-right">
            <div class="inline-flex items-center px-3 py-1 rounded-full bg-slate-100 text-slate-600 text-xs font-bold uppercase tracking-wider mb-2">
                <span class="w-2 h-2 rounded-full bg-emerald-500 mr-2"></span> System Online
            </div>
            <div class="text-sm font-bold text-slate-700">{{ current_user.name }}</div>
        </div>
    </div>

    <!-- The Switchboard Grid -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
        
        <!-- Switch 1: Gatekeeper -->
        <a href="{{ url_for('uip_bp.secretary_intake', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border transition-all duration-300 {% if switch_gate == 'red' %}bg-red-50 border-red-200 shadow-[0_0_15px_rgba(239,68,68,0.15)]{% else %}bg-slate-50 border-slate-200 hover:border-slate-300{% endif %}">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-full flex items-center justify-center text-2xl {% if switch_gate == 'red' %}bg-red-100 text-red-600{% else %}bg-slate-200 text-slate-400 group-hover:text-slate-600{% endif %}">
                        <i class="fas fa-users-cog"></i>
                    </div>
                    {% if switch_gate == 'red' %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-red-600 text-white uppercase tracking-widest animate-pulse">
                        Urgent Action
                    </span>
                    {% else %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-slate-200 text-slate-500 uppercase tracking-widest">
                        Clear
                    </span>
                    {% endif %}
                </div>
                
                <h2 class="text-2xl font-black {% if switch_gate == 'red' %}text-red-900{% else %}text-slate-700{% endif %} mb-2">Gatekeeper</h2>
                <p class="{% if switch_gate == 'red' %}text-red-700{% else %}text-slate-500{% endif %} font-medium">
                    {% if switch_gate == 'red' %}
                    There are {{ open_claims|length }} pending access claims requiring your verification.
                    {% else %}
                    No users are currently waiting for access verification.
                    {% endif %}
                </p>
            </div>
            {% if switch_gate == 'red' %}
            <div class="h-1.5 w-full bg-red-600 absolute bottom-0 left-0"></div>
            {% endif %}
        </a>

        <!-- Switch 2: Resolutions -->
        <a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border transition-all duration-300 {% if switch_res == 'red' %}bg-red-50 border-red-200 shadow-[0_0_15px_rgba(239,68,68,0.15)]{% elif switch_res == 'amber' %}bg-amber-50 border-amber-200 shadow-[0_0_15px_rgba(245,158,11,0.15)]{% else %}bg-slate-50 border-slate-200 hover:border-slate-300{% endif %}">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-full flex items-center justify-center text-2xl {% if switch_res == 'red' %}bg-red-100 text-red-600{% elif switch_res == 'amber' %}bg-amber-100 text-amber-600{% else %}bg-slate-200 text-slate-400 group-hover:text-slate-600{% endif %}">
                        <i class="fas fa-file-signature"></i>
                    </div>
                    {% if switch_res == 'red' %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-red-600 text-white uppercase tracking-widest animate-pulse">
                        Ratification Req
                    </span>
                    {% elif switch_res == 'amber' %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-amber-500 text-white uppercase tracking-widest">
                        Voting Active
                    </span>
                    {% else %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-slate-200 text-slate-500 uppercase tracking-widest">
                        Clear
                    </span>
                    {% endif %}
                </div>
                
                <h2 class="text-2xl font-black {% if switch_res == 'red' %}text-red-900{% elif switch_res == 'amber' %}text-amber-900{% else %}text-slate-700{% endif %} mb-2">Resolutions</h2>
                <p class="{% if switch_res == 'red' %}text-red-700{% elif switch_res == 'amber' %}text-amber-700{% else %}text-slate-500{% endif %} font-medium">
                    {% if switch_res == 'red' %}
                    {{ tabled_res }} resolution(s) have been TABLED and require manual ratification.
                    {% elif switch_res == 'amber' %}
                    {{ proposed_res }} resolution(s) are currently waiting for committee votes.
                    {% else %}
                    No active resolutions. Committee governance is up to date.
                    {% endif %}
                </p>
            </div>
            {% if switch_res == 'red' %}
            <div class="h-1.5 w-full bg-red-600 absolute bottom-0 left-0"></div>
            {% elif switch_res == 'amber' %}
            <div class="h-1.5 w-full bg-amber-500 absolute bottom-0 left-0"></div>
            {% endif %}
        </a>

        <!-- Switch 3: Initialize Resolution -->
        <a href="{{ url_for('uip_bp.draft_resolution', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-emerald-50 border-emerald-200 transition-all duration-300 hover:shadow-[0_0_15px_rgba(16,185,129,0.15)]">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-full flex items-center justify-center text-2xl bg-emerald-100 text-emerald-600">
                        <i class="fas fa-plus"></i>
                    </div>
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-emerald-600 text-white uppercase tracking-widest">
                        System Tool
                    </span>
                </div>
                
                <h2 class="text-2xl font-black text-emerald-900 mb-2">Draft Resolution</h2>
                <p class="text-emerald-700 font-medium">
                    Initialize a new blank document on the drafting desk.
                </p>
            </div>
            <div class="h-1.5 w-full bg-emerald-600 absolute bottom-0 left-0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
        </a>

        <!-- Switch 4: Committee Roster -->
        <a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-slate-50 border-slate-200 hover:border-slate-300 transition-all duration-300">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-full flex items-center justify-center text-2xl bg-slate-200 text-slate-500 group-hover:text-slate-600 transition-colors">
                        <i class="fas fa-id-badge"></i>
                    </div>
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-slate-200 text-slate-500 uppercase tracking-widest">
                        Directory
                    </span>
                </div>
                
                <h2 class="text-2xl font-black text-slate-700 mb-2">Committee Roster</h2>
                <p class="text-slate-500 font-medium">
                    View the active committee health and ExCo members.
                </p>
            </div>
        </a>

    </div>
</div>
{% endblock %}"""
with open("templates/program_uip/dashboards/secretary_workspace.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Built switchboard")
