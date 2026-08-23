import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace View Client Ledger logic
ledger_button_old = '''            {% if client_debtor %}
            <a href="{{ url_for('mechanic_bp.client_ledger', debtor_id=client_debtor.id) }}" class="px-4 py-2 border-2 border-indigo-600 text-indigo-700 bg-indigo-50 font-bold rounded-lg hover:bg-indigo-100 shadow-sm transition text-sm">View Client Ledger</a>
            {% endif %}'''

ledger_button_new = '''            {% if job_card.status == 'Quote' %}
                <form method="POST" action="{{ url_for('mechanic_bp.accept_quote', id=job_card.id) }}" class="inline m-0">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                    <button type="submit" class="px-4 py-2 bg-green-600 text-white font-bold rounded-lg hover:bg-green-700 shadow-sm transition text-sm">Accept Quote & Create Tax Invoice</button>
                </form>
            {% endif %}
            {% if client_debtor and job_card.status != 'Quote' %}
            <a href="{{ url_for('mechanic_bp.client_ledger', debtor_id=client_debtor.id) }}" class="px-4 py-2 border-2 border-indigo-600 text-indigo-700 bg-indigo-50 font-bold rounded-lg hover:bg-indigo-100 shadow-sm transition text-sm">View Client Ledger</a>
            {% endif %}'''

content = content.replace(ledger_button_old, ledger_button_new)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
