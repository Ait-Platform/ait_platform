import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''        {% if job_card.next_service_due %}
        <div style="margin-top: 20px; padding: 15px; background-color: #f8fafc; border: 1px dashed #cbd5e1; font-size: 14px; font-weight: bold; text-align: center; color: #334155; border-radius: 6px;">
            NEXT SERVICE DUE: {{ job_card.next_service_due }}
        </div>
        {% endif %}
        
        <div style="margin-top: 30px; font-size: 10px; color: #9ca3af; font-style: italic;">E.&O.E.</div>

        <div class="footer">'''

content = content.replace('<div class="footer">', replacement)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
