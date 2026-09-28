import sys

with open("templates/program_uip/dashboards/secretary_intake.html", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    '<th class="p-4 font-semibold">Date Submitted</th>\n                        </tr>',
    '<th class="p-4 font-semibold">Date Submitted</th>\n                            <th class="p-4 font-semibold">Actions</th>\n                        </tr>'
)

c = c.replace(
    '''<td class="p-4 text-sm text-slate-500">{{ claim.created_at.strftime('%Y-%m-%d %H:%M') }}</td>
                        </tr>''',
    '''<td class="p-4 text-sm text-slate-500">{{ claim.created_at.strftime('%Y-%m-%d %H:%M') }}</td>
                            <td class="p-4">
                                <button type="submit" formmethod="POST" formaction="{{ url_for('uip_bp.decline_claim', org_slug=org.slug, claim_id=claim.id) }}" class="text-red-500 hover:text-red-700 font-medium" title="Remove Claim" onclick="return confirm('Are you sure you want to remove this pending verification?');">
                                    <i class="fas fa-trash-alt mr-1"></i> Remove
                                </button>
                            </td>
                        </tr>'''
)

with open("templates/program_uip/dashboards/secretary_intake.html", "w", encoding="utf-8") as f:
    f.write(c)
