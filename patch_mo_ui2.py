with open("templates/program_uip/dashboards/municipal_officer.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We need to replace the entire Action column TD for tickets.
old_td = """<td class="p-4">
                                <span class="text-xs text-slate-400">Placeholder Action</span>
                            </td>"""
                            
new_td = """<td class="p-4">
                                <div class="flex items-center gap-2">
                                    <form method="POST" action="{{ url_for('uip_bp.mo_ticket_action', org_slug=org.slug, referral_id=referral.id) }}" class="inline-flex gap-2">
                                        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                                        {% if referral.status == 'ESCALATED_TO_MO' %}
                                        <button type="submit" name="action" value="acknowledge" class="px-3 py-1 bg-indigo-50 hover:bg-indigo-100 text-indigo-700 text-xs font-bold rounded border border-indigo-200 transition">Acknowledge</button>
                                        {% elif referral.status == 'ACKNOWLEDGED' %}
                                        <button type="submit" name="action" value="dispatch" class="px-3 py-1 bg-amber-50 hover:bg-amber-100 text-amber-700 text-xs font-bold rounded border border-amber-200 transition">Dispatch Team</button>
                                        {% elif referral.status == 'DISPATCHED' %}
                                        <button type="submit" name="action" value="resolve" class="px-3 py-1 bg-emerald-50 hover:bg-emerald-100 text-emerald-700 text-xs font-bold rounded border border-emerald-200 transition">Mark Resolved</button>
                                        {% endif %}
                                    </form>
                                    
                                    {% if referral.interaction.documents %}
                                    <div class="ml-2 pl-2 border-l border-slate-200 flex gap-2">
                                        {% for doc in referral.interaction.documents %}
                                            {% if doc.category == 'RP_QUERY_PHOTO' %}
                                            <a href="{{ url_for('uip_bp.document_download', org_slug=org.slug, document_id=doc.id, version=doc.current_version) }}" target="_blank" class="text-slate-500 hover:text-indigo-600 transition" title="View Evidence Photo">
                                                <i class="fas fa-camera"></i>
                                            </a>
                                            {% endif %}
                                        {% endfor %}
                                    </div>
                                    {% endif %}
                                </div>
                            </td>"""

text = text.replace(old_td, new_td)

with open("templates/program_uip/dashboards/municipal_officer.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated UI actions")
