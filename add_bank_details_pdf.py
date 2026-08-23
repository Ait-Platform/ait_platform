import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

bank_details_html = '''        
        {% if shop and shop.bank_details %}
        <div style="margin-top: 40px; padding: 20px; background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; font-size: 14px; color: #334155;">
            <h4 style="margin-top: 0; margin-bottom: 8px; color: #1e293b; font-size: 16px;">Bank Details</h4>
            <div style="white-space: pre-line;">{{ shop.bank_details }}</div>
        </div>
        {% endif %}
        '''

content = content.replace(
    '{% if shop and shop.terms_and_conditions %}',
    bank_details_html + '\n        {% if shop and shop.terms_and_conditions %}'
)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
