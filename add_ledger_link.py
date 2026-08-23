import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the mark_billed form block with a Ledger link
form_regex = r'<form method="POST" action="\{\{ url_for\(\'mechanic_bp\.mark_billed\', id=job\.id\) \}\}".*?</form>'
ledger_link = '''
                        {% set client_d = namespace(id=0) %}
                        {% for d in all_debtors %}
                          {% if d.name == job.vehicle.client.name %}
                            {% set client_d.id = d.id %}
                          {% endif %}
                        {% endfor %}
                        {% if client_d.id != 0 %}
                        <a href="{{ url_for('mechanic_bp.client_ledger', debtor_id=client_d.id) }}" class="inline m-0 ml-1 text-slate-700 hover:text-white font-semibold bg-slate-200 px-3 py-1 rounded-md transition hover:bg-slate-800 border border-slate-300 shadow-sm" title="Go to Client Ledger to record payments">
                          <i class="fas fa-file-invoice-dollar mr-1"></i> Ledger
                        </a>
                        {% endif %}
'''

content = re.sub(form_regex, ledger_link, content, flags=re.DOTALL)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
