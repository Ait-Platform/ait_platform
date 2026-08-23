import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '<div class="grid grid-cols-1 md:grid-cols-2 gap-6">',
    '<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">'
)

replacement = '''          <div class="bg-white border border-slate-200 rounded-xl p-5 shadow-sm relative group">
            <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
              <h3 class="text-sm font-bold text-slate-500 uppercase tracking-wider">Banking Details</h3>
              <a href="{{ url_for('mechanic_bp.bank_accounts') }}" class="text-xs font-semibold text-indigo-600 hover:text-indigo-800 transition px-2 py-1 bg-indigo-50 rounded hidden group-hover:block border border-indigo-200" target="_blank">
                <i class="fas fa-edit mr-1"></i>Edit
              </a>
            </div>
            {% if bank_account %}
                {% if bank_account.raw_details %}
                    <p class="text-slate-600 text-sm whitespace-pre-wrap">{{ bank_account.raw_details }}</p>
                {% else %}
                    <p class="text-slate-600 text-sm"><span class="font-semibold">Bank:</span> {{ bank_account.bank_name }}</p>
                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Account:</span> {{ bank_account.account_name }}</p>
                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Account No:</span> {{ bank_account.account_number }}</p>
                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">BSB:</span> {{ bank_account.bsb_branch }}</p>
                {% endif %}
            {% elif shop and shop.bank_details %}
                <p class="text-slate-600 text-sm whitespace-pre-wrap">{{ shop.bank_details }}</p>
            {% else %}
                <p class="text-slate-500 text-sm italic">No bank details configured. <a href="{{ url_for('mechanic_bp.bank_accounts') }}" class="text-indigo-600 hover:underline">Add one</a>.</p>
            {% endif %}
          </div>
        </div>'''

content = content.replace(
    '''            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Next Service Due:</span> {{ job_card.next_service_due if job_card.next_service_due else 'N/A' }}</p>
          </div>
        </div>''',
    '''            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Next Service Due:</span> {{ job_card.next_service_due if job_card.next_service_due else 'N/A' }}</p>
          </div>\n''' + replacement
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
