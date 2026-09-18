import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# Update header status badges to handle ESCALATED
old_header_status = """<span class="inline-block px-3 py-1 rounded font-bold text-xs uppercase tracking-widest {% if resolution.status == 'PROPOSED' %}bg-amber-100 text-amber-800{% elif resolution.status == 'ADOPTED' %}bg-emerald-100 text-emerald-800{% else %}bg-slate-100 text-slate-800{% endif %}">
                {{ resolution.status }}
            </span>"""
new_header_status = """<span class="inline-block px-3 py-1 rounded font-bold text-xs uppercase tracking-widest {% if resolution.status == 'PROPOSED' %}bg-indigo-100 text-indigo-800{% elif resolution.status == 'ADOPTED' %}bg-emerald-100 text-emerald-800{% elif resolution.status == 'ESCALATED' %}bg-red-100 text-red-800{% else %}bg-slate-100 text-slate-800{% endif %}">
                {{ resolution.status }}
            </span>"""
text = text.replace(old_header_status, new_header_status)

# Insert the Escalation Banner and Override Panel
escalation_block = """
        {% if resolution.status == 'ESCALATED' %}
        <div class="mb-6 bg-red-50 border border-red-200 rounded-xl p-6 shadow-sm">
            <h3 class="font-bold text-red-900 mb-2 text-lg"><i class="fas fa-exclamation-triangle mr-2"></i> Digital Voting Suspended</h3>
            <p class="text-sm text-red-700">A dissenting view (NAY) was logged. In accordance with round-robin governance rules, this resolution has been escalated and must be tabled at a formal live meeting.</p>
        </div>
        
        {% if current_appointment and current_appointment.position.lower() in ["secretary", "chairman", "vice chairman", "chair", "chairperson", "vice chair"] %}
        <div class="mb-6 bg-slate-900 rounded-xl p-6 shadow-lg border border-slate-800 text-white flex items-center justify-between">
            <div>
                <h3 class="font-bold text-lg mb-1"><i class="fas fa-gavel text-amber-500 mr-2"></i> Post-Meeting Ratification</h3>
                <p class="text-slate-400 text-sm">After the live meeting concludes, formally record the outcome here.</p>
            </div>
            <div class="flex gap-3">
                <form method="POST" action="{{ url_for('uip_bp.decide_resolution', org_slug=org.slug, res_id=resolution.id) }}">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" name="decision" value="REJECTED" class="px-4 py-2 bg-red-900 hover:bg-red-800 text-red-100 rounded-lg font-bold transition">
                        Reject
                    </button>
                    <button type="submit" name="decision" value="ADOPTED" class="px-6 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg font-bold shadow transition">
                        <i class="fas fa-check-double mr-1"></i> Adopt
                    </button>
                </form>
            </div>
        </div>
        {% endif %}
        {% endif %}
"""
# Insert after <div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
target = '<div class="md:col-span-2 space-y-6">'
text = text.replace(target, escalation_block + target)

# Also update the Right column Status block to handle ESCALATED
# Wait, if it's ESCALATED, it's not PROPOSED, so it falls into the Historical Record "else" block. Let's fix that.
old_right = """            {% else %}
            <div class="bg-slate-50 border border-slate-200 rounded-xl shadow-sm p-6 text-center">
                <i class="fas fa-landmark text-4xl text-slate-300 mb-4 block"></i>
                <h3 class="font-bold text-slate-700 mb-1">Historical Record</h3>
                <p class="text-sm text-slate-500">This resolution is officially adopted and closed for voting.</p>
            </div>
            {% endif %}"""

new_right = """            {% elif resolution.status == 'ESCALATED' %}
            <div class="bg-red-50 border border-red-200 rounded-xl shadow-sm p-6 text-center">
                <i class="fas fa-users text-4xl text-red-300 mb-4 block"></i>
                <h3 class="font-bold text-red-900 mb-1">Live Meeting Required</h3>
                <p class="text-sm text-red-700">Digital round-robin voting has been blocked.</p>
            </div>
            {% else %}
            <div class="bg-slate-50 border border-slate-200 rounded-xl shadow-sm p-6 text-center">
                <i class="fas fa-landmark text-4xl text-slate-300 mb-4 block"></i>
                <h3 class="font-bold text-slate-700 mb-1">Historical Record</h3>
                <p class="text-sm text-slate-500">This resolution is finalized and closed for voting.</p>
            </div>
            {% endif %}"""
text = text.replace(old_right, new_right)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_view.html for ESCALATED")
