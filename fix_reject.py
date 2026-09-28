import re

filepath = 'templates/program_uip/dashboards/secretary_intake.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''                    <td class="p-4">
                        <button type="button" onclick="openVerifyModal('{{ claim.id }}', '{{ claim.user_name }}')" class="inline-flex items-center px-4 py-2 border border-emerald-500 text-emerald-700 bg-emerald-50 hover:bg-emerald-100 font-bold text-sm rounded transition shadow-sm">
                            <i class="fas fa-check-circle mr-2"></i> Verify
                        </button>
                    </td>'''

replace_code = '''                    <td class="p-4">
                        <form method="POST" class="inline flex items-center">
                            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                            <button type="button" onclick="openVerifyModal('{{ claim.id }}', '{{ claim.user_name }}')" class="inline-flex items-center px-4 py-2 border border-emerald-500 text-emerald-700 bg-emerald-50 hover:bg-emerald-100 font-bold text-sm rounded transition shadow-sm mr-2">
                                <i class="fas fa-check-circle mr-2"></i> Verify
                            </button>
                            <button type="submit" formaction="{{ url_for('uip_bp.decline_claim', org_slug=org.slug, claim_id=claim.id) }}" class="inline-flex items-center px-3 py-2 text-red-500 hover:text-red-700 hover:bg-red-50 font-bold text-sm rounded transition shadow-sm border border-transparent hover:border-red-200" title="Reject Request" onclick="return confirm('Are you sure you want to permanently decline this access request?');">
                                <i class="fas fa-times"></i>
                            </button>
                        </form>
                    </td>'''

content = content.replace(search_code, replace_code)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Added reject button to top table")
