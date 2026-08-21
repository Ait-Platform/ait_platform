import sys

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''            {% if job_card.status in ['Approved', 'In Progress', 'Quality Check', 'Ready', 'Billed'] %}
              <a href="{{ url_for('mechanic_bp.client_soa', client_id=job_card.vehicle.client.id, return_url=url_for('mechanic_bp.job_card_detail', id=job_card.id)) }}" class="inline-flex items-center rounded-lg border border-indigo-200 bg-white px-4 py-2 text-sm font-semibold text-indigo-700 shadow-sm hover:bg-slate-50 transition">
                View Client SOA
              </a>
            {% endif %}
'''

if target in content:
    content = content.replace(target, '')
    print("Found and removed.")
else:
    print("Not found.")

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
