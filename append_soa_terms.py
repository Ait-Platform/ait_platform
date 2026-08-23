import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    </table>
    
    {% if shop and shop.terms_and_conditions %}
    <div style="margin-top: 40px; padding: 20px; border-top: 1px solid #e5e7eb; font-size: 11px; color: #6b7280;">
        <h4 style="margin-top: 0; margin-bottom: 8px; color: #374151;">Terms & Conditions</h4>
        <div style="white-space: pre-line;">{{ shop.terms_and_conditions }}</div>
    </div>
    {% endif %}
    
    <div style="margin-top: 30px; font-size: 10px; color: #9ca3af; font-style: italic;">E.&O.E.</div>'''

content = content.replace('    </table>', replacement)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
