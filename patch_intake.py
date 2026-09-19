widget = """
<!-- PRECINCT ACTIVATION HEALTH -->
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
"""

with open("templates/program_uip/dashboards/secretary_intake.html", "r", encoding="utf-8") as f:
    text = f.read()

target = "{% include 'partials/flash_messages.html' %}\n\n"
text = text.replace(target, target + widget)

with open("templates/program_uip/dashboards/secretary_intake.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Injected quorum widget into secretary intake")
