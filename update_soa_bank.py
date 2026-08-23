import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''            {% if bank_account or (profile and profile.bank_details) %}
            <div class="mt-4 text-left border-t border-gray-200 pt-3">
                <h3 class="text-xs uppercase font-bold text-gray-500 tracking-wider mb-1">Payment Details / Bank Info:</h3>
                {% if profile and profile.bank_details %}
                    <p class="text-sm text-gray-700 whitespace-pre-line">{{ profile.bank_details }}</p>
                {% elif bank_account.raw_details %}
                    <p class="text-sm text-gray-700 whitespace-pre-line">{{ bank_account.raw_details }}</p>
                {% else %}
                    <div class="text-sm text-gray-700">
                        <div><strong>Bank:</strong> {{ bank_account.bank_name }}</div>
                        <div><strong>Account Name:</strong> {{ bank_account.account_name }}</div>
                        <div><strong>BSB:</strong> {{ bank_account.bsb_branch }} &nbsp; <strong>Account No:</strong> {{ bank_account.account_number }}</div>
                        {% if bank_account.swift_code %}<div><strong>SWIFT:</strong> {{ bank_account.swift_code }}</div>{% endif %}
                    </div>
                {% endif %}
                <div class="mt-2 text-sm text-gray-700"><strong>Payment Reference:</strong><br>{% if latest_job_card %}Job Card #{{ latest_job_card.job_number }}{% else %}Account: {{ debtor.name }}{% endif %}</div>
            </div>
            {% endif %}'''

content = re.sub(
    r"            \{% if bank_account %\}\s*<div class=\"mt-4\">\s*<h3 class=\"text-xs uppercase font-bold text-gray-500 tracking-wider mb-1\">Payment Details / Bank Info:</h3>\s*\{% if bank_account\.raw_details %\}\s*<p class=\"text-sm text-gray-700 whitespace-pre-line\">\{\{ bank_account\.raw_details \}\}</p>\s*\{% else %\}\s*<div class=\"text-sm text-gray-700\">\s*<div><strong>Bank:</strong> \{\{ bank_account\.bank_name \}\}</div>\s*<div><strong>Account Name:</strong> \{\{ bank_account\.account_name \}\}</div>\s*<div><strong>BSB:</strong> \{\{ bank_account\.bsb_branch \}\} &nbsp; <strong>Account No:</strong> \{\{ bank_account\.account_number \}\}</div>\s*\{% if bank_account\.swift_code %\}<div><strong>SWIFT:</strong> \{\{ bank_account\.swift_code \}\}</div>\{% endif %\}\s*</div>\s*\{% endif %\}\s*</div>\s*\{% endif %\}",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
