import re

filepath = 'templates/program_uip/dashboards/resolution_view.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Add Endorsements display and button inside the Official Ratification Record block
rat_block_end = '''            <div>
                <div class="text-[10px] uppercase tracking-widest font-bold text-slate-400 mb-1">Recorded By</div>
                <div class="font-bold text-slate-700">{{ rat.recorded_by_name }}</div>
                <div class="text-xs text-slate-500">{{ rat.recorded_by_email }}</div>
            </div>
        </div>'''

endorsement_html = '''            <div>
                <div class="text-[10px] uppercase tracking-widest font-bold text-slate-400 mb-1">Recorded By</div>
                <div class="font-bold text-slate-700">{{ rat.recorded_by_name }}</div>
                <div class="text-xs text-slate-500">{{ rat.recorded_by_email }}</div>
            </div>
            
            <div class="md:col-span-4 mt-2 pt-4 border-t border-slate-200 flex justify-between items-center">
                <div>
                    <div class="text-[10px] uppercase tracking-widest font-bold text-slate-400 mb-1">ExCo Endorsements (Safeguard)</div>
                    <div class="flex flex-wrap gap-2">
                        {% if rat.endorsements %}
                            {% for e in rat.endorsements %}
                            <span class="inline-flex items-center px-2 py-1 rounded bg-emerald-100 text-emerald-800 text-xs font-bold" title="{{ e.email }} at {{ e.timestamp }}">
                                <i class="fas fa-check-circle mr-1"></i> {{ e.name }}
                            </span>
                            {% endfor %}
                        {% else %}
                            <span class="text-xs text-slate-500 italic">No endorsements yet.</span>
                        {% endif %}
                    </div>
                </div>
                
                {% set has_endorsed = false %}
                {% if rat.endorsements %}
                    {% for e in rat.endorsements %}
                        {% if e.email == current_user.email %}
                            {% set has_endorsed = true %}
                        {% endif %}
                    {% endfor %}
                {% endif %}
                
                {% if current_appointment and current_appointment.position.lower() in ["chairman", "chairperson", "chair", "vice chair", "vice chairman", "secretary", "treasurer"] and rat.recorded_by_email != current_user.email and not has_endorsed %}
                <form method="POST" action="{{ url_for('uip_bp.endorse_ratification', org_slug=org.slug, res_id=resolution.id) }}">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" class="px-4 py-2 bg-emerald-500 hover:bg-emerald-600 text-white text-sm font-bold rounded shadow-sm transition inline-flex items-center">
                        <i class="fas fa-check-double mr-2"></i> Endorse Record
                    </button>
                </form>
                {% endif %}
            </div>
        </div>'''

content = content.replace(rat_block_end, endorsement_html)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("resolution_view updated")
