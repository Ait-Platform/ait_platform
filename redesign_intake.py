import re

filepath = 'templates/program_uip/dashboards/secretary_intake.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace header description
old_header = '''<p class="ui-subtitle">Review access claims and draft digital resolutions for committee approval.</p>'''
new_header = '''<p class="ui-subtitle text-slate-600 mt-1">
            <strong>Path 1 (Instant):</strong> Click "Verify via Mandate" to link an applicant to an already-adopted mandate.<br>
            <strong>Path 2 (Voting):</strong> Select applicants with checkboxes and click "Draft Resolution" to send them to the voting room.
        </p>'''
content = content.replace(old_header, new_header)

# Fix the checkbox logic so MOs get a checkbox too!
old_checkbox = '''{% if claim.type != 'mo_claim' %}
                                <input type="checkbox" name="claim_ids[]" value="{{ claim.id }}" class="claim-checkbox rounded border-slate-300 text-indigo-600 focus:ring-indigo-500">
                                {% else %}
                                <i class="fas fa-star text-amber-400" title="Mandate requires direct recording"></i>
                                {% endif %}'''
new_checkbox = '''<input type="checkbox" name="claim_ids[]" value="{{ claim.id }}" class="claim-checkbox rounded border-slate-400 text-indigo-600 focus:ring-indigo-500 cursor-pointer w-5 h-5">'''
content = content.replace(old_checkbox, new_checkbox)

# Make the action buttons cleaner
old_actions = '''<button type="button" onclick="openVerifyModal('{{ claim.id }}', '{{ claim.user_name }}')" class="text-indigo-600 hover:text-indigo-900 font-bold mr-4 transition">
                                    <i class="fas fa-link mr-1"></i> Verify via Mandate
                                </button>
                                <button type="submit" formmethod="POST" formaction="{{ url_for('uip_bp.decline_claim', org_slug=org.slug, claim_id=claim.id) }}" class="text-red-500 hover:text-red-700 font-medium" title="Remove Claim" onclick="return confirm('Are you sure you want to remove this pending verification?');">
                                    <i class="fas fa-trash-alt mr-1"></i> Remove
                                </button>'''
new_actions = '''<button type="button" onclick="openVerifyModal('{{ claim.id }}', '{{ claim.user_name }}')" class="inline-flex items-center px-3 py-1.5 border border-emerald-500 text-emerald-700 bg-emerald-50 hover:bg-emerald-100 font-bold text-xs rounded transition mr-2">
                                    <i class="fas fa-link mr-1.5"></i> Verify via Mandate
                                </button>
                                <button type="submit" formmethod="POST" formaction="{{ url_for('uip_bp.decline_claim', org_slug=org.slug, claim_id=claim.id) }}" class="inline-flex items-center px-3 py-1.5 text-red-500 hover:text-red-700 hover:bg-red-50 font-medium text-xs rounded transition" title="Reject Request" onclick="return confirm('Are you sure you want to permanently decline this access request?');">
                                    <i class="fas fa-times mr-1.5"></i> Reject
                                </button>'''
content = content.replace(old_actions, new_actions)

# Make Draft Button more explicit
old_draft_btn = '''<button type="submit" class="ui-btn ui-btn-primary">
                    <i class="fas fa-file-signature mr-2"></i> Draft Access Resolution
                </button>'''
new_draft_btn = '''<div class="flex items-center text-slate-500 text-sm mr-4">
                    <i class="fas fa-level-up-alt fa-rotate-90 mr-2 text-slate-400"></i> No mandate? Select applicants to send to the voting room.
                </div>
                <button type="submit" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2.5 px-6 rounded-lg shadow-sm transition inline-flex items-center">
                    <i class="fas fa-balance-scale mr-2"></i> Draft Resolution for Vote
                </button>'''
content = content.replace(old_draft_btn, new_draft_btn)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Redesigned secretary_intake.html")
