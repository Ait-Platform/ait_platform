import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

terms_html = '''
        {% if shop and shop.terms_and_conditions %}
        <div style="margin-top: 40px; padding: 20px; border-top: 1px solid #e5e7eb; font-size: 11px; color: #6b7280;">
            <h4 style="margin-top: 0; margin-bottom: 8px; color: #374151;">Terms & Conditions</h4>
            <div style="white-space: pre-line;">{{ shop.terms_and_conditions }}</div>
        </div>
        {% endif %}
'''

# insert before <div class="footer">
content = content.replace('<div class="footer">', terms_html + '\n        <div class="footer">')

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
