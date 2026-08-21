import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_link = '''<a href="{{ url_for('debtors_bp.generate_soa', debtor_id=d.id) }}" class="text-white bg-red-600 hover:bg-red-700 px-3 py-1 rounded-md transition shadow-sm">
                    View SOA &rarr;
                  </a>'''

new_link = '''<a href="{{ url_for('debtors_bp.generate_soa', debtor_id=d.id, return_url=url_for('mechanic_bp.job_cards_list')) }}" class="text-white bg-red-600 hover:bg-red-700 px-3 py-1 rounded-md transition shadow-sm">
                    View SOA &rarr;
                  </a>'''

content = content.replace(old_link, new_link)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
