import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace View in Debtors link
old_link = '''<a href="{{ url_for('debtors_bp.generate_soa', debtor_id=client_debtor.id) }}" class="px-4 py-2 border-2 border-indigo-600 text-indigo-700 bg-indigo-50 font-bold rounded-lg hover:bg-indigo-100 shadow-sm transition text-sm">View in Debtors</a>'''

new_link = '''<a href="{{ url_for('debtors_bp.generate_soa', debtor_id=client_debtor.id, return_url=url_for('mechanic_bp.job_card_detail', id=job_card.id)) }}" class="px-4 py-2 border-2 border-indigo-600 text-indigo-700 bg-indigo-50 font-bold rounded-lg hover:bg-indigo-100 shadow-sm transition text-sm">View in Debtors</a>'''

content = content.replace(old_link, new_link)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
