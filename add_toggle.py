import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# I will inject the Payment Method toggle right below the status pill.
regex = r'(<span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold\s*\{% if job_card\.status == \'Billed\' %\}.*?\{\% endif \%\}">\s*\{\{ job_card\.status \}\}\s*</span>)'

toggle_html = '''\\1
            
            {% if job_card.status == 'Quote' %}
            <div class="mt-4 flex items-center gap-3 bg-white p-3 rounded-lg border border-slate-200 shadow-sm inline-block">
              <span class="text-sm font-bold text-slate-700"><i class="fas fa-money-bill-wave text-emerald-500 mr-2"></i>Requested Payment:</span>
              <form method="POST" action="{{ url_for('mechanic_bp.update_payment_method', id=job_card.id) }}" class="inline-flex">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                <select name="payment_method" onchange="this.form.submit()" class="text-sm font-semibold text-slate-900 border-0 bg-slate-100 rounded-md py-1 pl-3 pr-8 focus:ring-2 focus:ring-indigo-500 cursor-pointer">
                  <option value="EFT" {% if job_card.payment_method == 'EFT' or not job_card.payment_method %}selected{% endif %}>Bank Transfer (EFT)</option>
                  <option value="eWallet" {% if job_card.payment_method == 'eWallet' %}selected{% endif %}>eWallet / Send Cash</option>
                  <option value="Cash" {% if job_card.payment_method == 'Cash' %}selected{% endif %}>Physical Cash</option>
                </select>
              </form>
            </div>
            {% else %}
            <div class="mt-4 flex items-center gap-2">
              <span class="text-sm font-bold text-slate-500 uppercase tracking-wider">Payment Method:</span>
              <span class="text-sm font-bold text-slate-900">
                {% if job_card.payment_method == 'eWallet' %}
                  <i class="fas fa-mobile-alt text-indigo-500 mr-1"></i> eWallet / Send Cash
                {% elif job_card.payment_method == 'Cash' %}
                  <i class="fas fa-money-bill-wave text-emerald-500 mr-1"></i> Physical Cash
                {% else %}
                  <i class="fas fa-university text-blue-500 mr-1"></i> Bank Transfer (EFT)
                {% endif %}
              </span>
            </div>
            {% endif %}
'''

content = re.sub(regex, toggle_html, content, flags=re.DOTALL)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
