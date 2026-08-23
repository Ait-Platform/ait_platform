import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''        
        {% if bank_account %}
        <div style="margin-top: 40px; padding: 20px; background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; font-size: 14px; color: #334155;">
            <h4 style="margin-top: 0; margin-bottom: 8px; color: #1e293b; font-size: 16px;">Bank Details</h4>
            {% if bank_account.raw_details %}
            <div style="white-space: pre-line;">{{ bank_account.raw_details }}</div>
            {% else %}
            <div>
                <strong>Bank:</strong> {{ bank_account.bank_name }}<br>
                <strong>Account Name:</strong> {{ bank_account.account_name }}<br>
                <strong>BSB / Branch:</strong> {{ bank_account.bsb_branch }}<br>
                <strong>Account No:</strong> {{ bank_account.account_number }}<br>
                {% if bank_account.swift_code %}<strong>SWIFT:</strong> {{ bank_account.swift_code }}<br>{% endif %}
            </div>
            {% endif %}
        </div>
        {% endif %}
        '''

content = re.sub(
    r"\s*\{% if shop and shop\.bank_details %\}.*?\{% endif %\}\s*",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
