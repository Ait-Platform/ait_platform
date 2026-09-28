import re

filepath = 'templates/program_uip/dashboards/secretary_intake.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace bottom table logic
search_html = '''                    <td class="p-4">
                        <form method="POST" action="{{ url_for('uip_bp.undo_claim', org_slug=org.slug, claim_id=claim.id) }}">
                            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                            <button type="submit" class="inline-flex items-center px-3 py-1.5 text-red-600 hover:text-red-800 hover:bg-red-50 font-bold text-xs rounded transition" onclick="return confirm('Remove this applicant from processing and return them to the waiting room?');">
                                <i class="fas fa-undo-alt mr-1.5"></i> Remove
                            </button>
                        </form>
                    </td>'''

replace_html = '''                    <td class="p-4">
                        {% if claim.can_undo %}
                        <form method="POST" action="{{ url_for('uip_bp.undo_claim', org_slug=org.slug, claim_id=claim.id) }}">
                            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                            <button type="submit" class="inline-flex items-center px-3 py-1.5 text-red-600 hover:text-red-800 hover:bg-red-50 font-bold text-xs rounded transition" onclick="return confirm('Remove this applicant from processing and return them to the waiting room?');">
                                <i class="fas fa-undo-alt mr-1.5"></i> Undo ({{ claim.minutes_left }}m)
                            </button>
                        </form>
                        {% else %}
                        <span class="text-xs text-slate-400 font-medium">
                            <i class="fas fa-lock mr-1"></i> Locked
                        </span>
                        {% endif %}
                    </td>'''

content = content.replace(search_html, replace_html)

# Add a small note below the table
search_note = '''    {% else %}
    <div class="ui-card-body text-center py-12">'''

replace_note = '''    <div class="px-4 py-3 bg-slate-50 border-t border-slate-200 text-xs text-slate-500">
        <i class="fas fa-info-circle mr-1"></i> You have a 15-minute grace period to undo recent processing. After 15 minutes, verification is locked. To replace verified members after this period, update their status to "Former" in the official Committee Register.
    </div>
    {% else %}
    <div class="ui-card-body text-center py-12">'''

content = content.replace(search_note, replace_note)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated UI with 15 minute timer")
