import re

with open('templates/admin/security/pricing.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add table header
content = content.replace(
    '<th class="px-4 py-3 font-medium text-slate-600">Configured Local Price</th>',
    '<th class="px-4 py-3 font-medium text-slate-600">Configured Local Price</th>\n                    <th class="px-4 py-3 font-medium text-slate-600">ZAR Equivalent (For Paystack)</th>'
)

# Add table cell
old_td = '''<td class="px-4 py-3">
                        <input type="number" name="amount_{{ p.country_code }}" value="{% if p.local_amount_cents %}{{ p.local_amount_cents // 100 }}{% endif %}" placeholder="e.g. 100" class="w-24 border border-slate-300 rounded-md p-1 outline-none text-sm">
                    </td>'''
new_td = '''<td class="px-4 py-3">
                        <input type="number" name="amount_{{ p.country_code }}" value="{% if p.local_amount_cents %}{{ p.local_amount_cents // 100 }}{% endif %}" placeholder="e.g. 100" class="w-24 border border-slate-300 rounded-md p-1 outline-none text-sm">
                    </td>
                    <td class="px-4 py-3 text-slate-500 font-mono">
                        {% if p.zar_amount_cents %}ZAR {{ p.zar_amount_cents // 100 }}{% else %}-{% endif %}
                    </td>'''

content = content.replace(old_td, new_td)

with open('templates/admin/security/pricing.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated admin pricing template")
