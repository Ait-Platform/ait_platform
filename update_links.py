import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<a href="{{ url_for('debtors_bp.dashboard', source='mechanic') }}" class="block text-center w-full rounded-xl border-2 border-sky-200 p-3 shadow-sm transition hover:shadow bg-sky-50 hover:border-sky-400 text-sky-900 font-semibold text-sm group">
                <i class="fas fa-file-invoice-dollar mr-1 group-hover:text-sky-700"></i> Debtors (SOA)
              </a>''',
    '''<a href="{{ url_for('mechanic_bp.client_accounts') }}" class="block text-center w-full rounded-xl border-2 border-indigo-200 p-3 shadow-sm transition hover:shadow bg-indigo-50 hover:border-indigo-400 text-indigo-900 font-semibold text-sm group">
                <i class="fas fa-file-invoice-dollar mr-1 group-hover:text-indigo-700"></i> Client Accounts (SOA)
              </a>'''
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<a href="{{ url_for('debtors_bp.generate_soa', debtor_id=d.id, return_url=url_for('mechanic_bp.job_cards_list')) }}" class="text-white bg-red-600 hover:bg-red-700 px-3 py-1 rounded-md transition shadow-sm">
                      View SOA &rarr;
                    </a>''',
    '''<a href="{{ url_for('mechanic_bp.client_ledger', debtor_id=d.id) }}" class="text-white bg-red-600 hover:bg-red-700 px-3 py-1 rounded-md transition shadow-sm font-semibold">
                      View Ledger &rarr;
                    </a>'''
)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)


with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<a href="{{ url_for('debtors_bp.generate_soa', debtor_id=client_debtor.id, return_url=url_for('mechanic_bp.job_card_detail', id=job_card.id)) }}" class="px-4 py-2 border-2 border-indigo-600 text-indigo-700 bg-indigo-50 font-bold rounded-lg hover:bg-indigo-100 shadow-sm transition text-sm">View in Debtors</a>''',
    '''<a href="{{ url_for('mechanic_bp.client_ledger', debtor_id=client_debtor.id) }}" class="px-4 py-2 border-2 border-indigo-600 text-indigo-700 bg-indigo-50 font-bold rounded-lg hover:bg-indigo-100 shadow-sm transition text-sm">View Client Ledger</a>'''
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)

