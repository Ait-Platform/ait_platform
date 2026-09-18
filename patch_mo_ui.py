import re

with open("templates/program_uip/dashboards/municipal_officer.html", "r", encoding="utf-8") as f:
    text = f.read()

start_idx = text.find('<td class="p-4">')
end_idx = text.find('</td>', start_idx) + len('</td>')

old_td = text[start_idx:end_idx]

new_td = """<td class="p-4">
                                <form method="POST" action="{{ url_for('uip_bp.mo_ticket_action', org_slug=org.slug, referral_id=referral.id) }}" class="inline flex flex-wrap gap-2">
                                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                                    
                                    {% if referral.status == 'ESCALATED_TO_MO' %}
                                    <button type="submit" name="action" value="acknowledge" class="ui-btn bg-slate-100 hover:bg-slate-200 text-slate-700 text-xs py-1.5 px-3 border border-slate-300">
                                        <i class="fas fa-eye mr-1"></i> Acknowledge
                                    </button>
                                    {% endif %}
                                    
                                    {% if referral.status in ['ESCALATED_TO_MO', 'ACKNOWLEDGED'] %}
                                    <button type="submit" name="action" value="dispatch" class="ui-btn bg-amber-100 hover:bg-amber-200 text-amber-800 text-xs py-1.5 px-3 border border-amber-300">
                                        <i class="fas fa-truck mr-1"></i> Dispatch City Team
                                    </button>
                                    {% endif %}
                                    
                                    {% if referral.status != 'RESOLVED' %}
                                    <button type="submit" name="action" value="resolve" class="ui-btn ui-btn-primary bg-emerald-600 hover:bg-emerald-700 text-white text-xs py-1.5 px-3">
                                        <i class="fas fa-check mr-1"></i> Resolve
                                    </button>
                                    {% else %}
                                    <span class="inline-block bg-slate-100 text-slate-500 text-xs font-bold px-3 py-1.5 rounded"><i class="fas fa-check-double mr-1"></i> Resolved</span>
                                    {% endif %}
                                </form>
                            </td>"""

text = text.replace(old_td, new_td)

# And update the query in routes to allow viewing acknowledged and dispatched tickets
# But let's first update the template
with open("templates/program_uip/dashboards/municipal_officer.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated MO template successfully!")
