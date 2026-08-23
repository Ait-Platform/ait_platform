import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''            <div class="col">
                <h3>Banking Details</h3>
                {% if bank_account %}
                    {% if bank_account.raw_details %}
                        <p style="font-size: 13px; white-space: pre-wrap;">{{ bank_account.raw_details }}</p>
                    {% else %}
                        <div style="font-size: 13px;">
                            <div><strong>Bank:</strong> {{ bank_account.bank_name }}</div>
                            <div><strong>Account Name:</strong> {{ bank_account.account_name }}</div>
                            <div><strong>BSB:</strong> {{ bank_account.bsb_branch }} &nbsp; <strong>Account No:</strong> {{ bank_account.account_number }}</div>
                            {% if bank_account.swift_code %}<div><strong>SWIFT:</strong> {{ bank_account.swift_code }}</div>{% endif %}
                        </div>
                    {% endif %}
                {% elif shop and shop.bank_details %}
                    <p style="font-size: 13px;">{{ shop.bank_details | replace('\n', '<br>') | safe }}</p>
                {% else %}
                    <p style="font-size: 13px;">Please contact us for payment details.</p>
                {% endif %}
                <p style="margin-top: 10px; font-size: 13px;"><strong>Payment Reference:</strong><br>Job Card #{{ job_card.job_number }}</p>
            </div>'''

content = re.sub(
    r"            <div class=\"col\">\s*<h3>Banking Details</h3>\s*\{% if shop and shop\.bank_details %\}\s*<p style=\"font-size: 13px;\">\{\{ shop\.bank_details \| replace\('\\n', '<br>'\) \| safe \}\}</p>\s*\{% else %\}\s*<p style=\"font-size: 13px;\">Please contact us for payment details\.</p>\s*\{% endif %\}\s*<p style=\"margin-top: 10px; font-size: 13px;\"><strong>Payment Reference:</strong><br>Job Card #\{\{ job_card\.job_number \}\}</p>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
