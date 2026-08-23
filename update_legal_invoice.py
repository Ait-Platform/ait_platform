import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Change title to Tax Invoice if not Quote/Rejected
content = content.replace(
    "{{ 'Tax Invoice' if job_card.status == 'Billed' else 'Quote' }}",
    "{{ 'Quote' if job_card.status in ['Quote', 'Rejected'] else 'Tax Invoice' }}"
)

# Add VAT and Reg numbers
header_replacement = '''            <h2>{{ 'Quote' if job_card.status in ['Quote', 'Rejected'] else 'Tax Invoice' }} #{{ job_card.job_number }}</h2>
            <p>Created: {{ job_card.created_at.strftime('%Y-%m-%d') }} | Status: {{ job_card.status }}</p>
            {% if shop %}
            <p style="font-size: 12px; color: #666; margin-top: 5px;">
              {% if shop.registration_number %}Reg: {{ shop.registration_number }} | {% endif %}
              {% if shop.tax_number %}VAT: {{ shop.tax_number }}{% endif %}
            </p>
            {% endif %}'''

content = re.sub(
    r"<h2>\{\{ 'Quote' if job_card\.status in \['Quote', 'Rejected'\] else 'Tax Invoice' \}\} #\{\{ job_card\.job_number \}\}</h2>\s*<p>Created: \{\{ job_card\.created_at\.strftime\('%Y-%m-%d'\) \}\} \| Status: \{\{ job_card\.status \}\}</p>",
    header_replacement,
    content
)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
