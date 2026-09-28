import re

filepath = 'templates/program_uip/dashboards/secretary_intake.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''                            <button type="submit" formaction="{{ url_for('uip_bp.decline_claim', org_slug=org.slug, claim_id=claim.id) }}" class="inline-flex items-center px-3 py-2 text-red-500 hover:text-red-700 hover:bg-red-50 font-bold text-sm rounded transition shadow-sm border border-transparent hover:border-red-200" title="Reject Request" onclick="return confirm('Are you sure you want to permanently decline this access request?');">
                                <i class="fas fa-times"></i>
                            </button>'''

replace_code = '''                            <button type="submit" formaction="{{ url_for('uip_bp.decline_claim', org_slug=org.slug, claim_id=claim.id) }}" class="inline-flex items-center px-4 py-2 border border-red-500 text-red-700 bg-red-50 hover:bg-red-100 font-bold text-sm rounded transition shadow-sm" title="Reject Request" onclick="return confirm('Are you sure you want to permanently decline this access request?');">
                                <i class="fas fa-times mr-2"></i> Reject
                            </button>'''

content = content.replace(search_code, replace_code)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated Reject button in top table")
