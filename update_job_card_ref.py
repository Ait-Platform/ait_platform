import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# I need to find the Banking Details block and add Payment Reference inside it.
banking_block = '''            {% if bank_account %}
                {% if bank_account.raw_details %}
                    <p class="text-slate-600 text-sm whitespace-pre-wrap">{{ bank_account.raw_details }}</p>
                {% else %}
                    <p class="text-slate-600 text-sm"><span class="font-semibold">Bank:</span> {{ bank_account.bank_name }}</p>
                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Account:</span> {{ bank_account.account_name }}</p>
                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">BSB:</span> {{ bank_account.bsb_branch }}</p>
                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Account No:</span> {{ bank_account.account_number }}</p>
                {% endif %}
            {% elif shop and shop.bank_details %}
                <p class="text-slate-600 text-sm whitespace-pre-wrap">{{ shop.bank_details }}</p>
            {% else %}
                <p class="text-slate-500 text-sm italic">No bank details configured. <a href="{{ url_for('mechanic_bp.bank_accounts') }}" class="text-indigo-600 hover:underline">Add one</a>.</p>
            {% endif %}'''

new_banking_block = banking_block + '''
            <p class="text-slate-600 text-sm mt-3"><span class="font-semibold">Payment Reference:</span><br>{{ job_card.job_number.split('-')[-1] }}</p>'''

content = content.replace(banking_block, new_banking_block)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
