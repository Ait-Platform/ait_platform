import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# I will use a simple regex or just find the main div and replace the rest of the file
start_idx = content.find('<div class="p-6">')
if start_idx != -1:
    new_body = '''<div class="p-6 space-y-8">
      
      <!-- DEBTORS WITH BALANCES -->
      {% if debtors_with_balances %}
      <div>
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-xl font-bold text-red-600"><i class="fas fa-exclamation-circle mr-2"></i> Debtors (Outstanding Balances)</h2>
        </div>
        <div class="border border-red-200 rounded-xl overflow-x-auto shadow-sm bg-red-50">
          <table class="min-w-full divide-y divide-red-200">
            <thead class="bg-red-100">
              <tr>
                <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-red-700 uppercase tracking-wider">Client</th>
                <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-red-700 uppercase tracking-wider">Email/Phone</th>
                <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-red-700 uppercase tracking-wider">Balance Owed</th>
                <th scope="col" class="px-6 py-3 text-right text-xs font-bold text-red-700 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-red-100">
              {% for d in debtors_with_balances %}
              <tr class="hover:bg-red-50 transition">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-bold text-slate-900">{{ d.name }}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{{ d.email or d.phone }}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-bold text-red-600">R {{ "%.2f"|format(d.current_balance) }}</td>
                <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                  <a href="{{ url_for('debtors_bp.generate_soa', debtor_id=d.id) }}" class="text-white bg-red-600 hover:bg-red-700 px-3 py-1 rounded-md transition shadow-sm">
                    View SOA &rarr;
                  </a>
                </td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
      {% endif %}

      <!-- MACRO FOR JOB CARD TABLES -->
      {% macro job_table(title, jobs, color, table_id) %}
      <div class="mt-8">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-xl font-bold text-slate-800">{{ title }} <span class="bg-slate-200 text-slate-700 px-2 py-0.5 rounded-full text-sm ml-2">{{ jobs|length }}</span></h2>
          {% if jobs|length > 3 %}
          <button onclick="toggleTable('{{ table_id }}', this)" class="text-sm font-semibold text-indigo-600 hover:text-indigo-800 focus:outline-none">
            Show More &darr;
          </button>
          {% endif %}
        </div>
        <div class="border border-slate-200 rounded-xl overflow-x-auto shadow-sm bg-white">
          <table class="min-w-full divide-y divide-slate-200">
            <thead class="bg-slate-50">
              <tr>
                <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Job #</th>
                <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Date</th>
                <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Client & Vehicle</th>
                <th scope="col" class="px-6 py-3 text-right text-xs font-bold text-slate-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody id="{{ table_id }}" class="bg-white divide-y divide-slate-200">
              {% for job in jobs %}
              <tr class="hover:bg-slate-50 transition {% if loop.index > 3 %}hidden expanded-row{% endif %}">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-slate-900">#{{ job.job_number }}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-500">{{ job.created_at.strftime('%Y-%m-%d') }}</td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <div class="text-sm font-bold text-slate-900">{{ job.vehicle.client.name }}</div>
                  <div class="text-sm text-slate-500">{{ job.vehicle.registration_number }}</div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                  <a href="{{ url_for('mechanic_bp.job_card_detail', id=job.id) }}" class="text-indigo-600 hover:text-indigo-900 font-semibold bg-indigo-50 px-3 py-1 rounded-md transition hover:bg-indigo-100 border border-indigo-100">
                    Open Hub &rarr;
                  </a>
                </td>
              </tr>
              {% else %}
              <tr>
                <td colspan="4" class="px-6 py-8 text-center text-slate-500 italic">No job cards in this category.</td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
      {% endmacro %}

      {% set pending = [] %}
      {% set accepted = [] %}
      {% set rejected = [] %}
      {% set completed = [] %}
      
      {% for j in job_cards %}
        {% if j.status == 'Quote' %}{% set _ = pending.append(j) %}
        {% elif j.status in ['Approved', 'Awaiting Deposit'] %}{% set _ = accepted.append(j) %}
        {% elif j.status == 'Rejected' %}{% set _ = rejected.append(j) %}
        {% else %}{% set _ = completed.append(j) %}
        {% endif %}
      {% endfor %}

      {{ job_table("Pending Quotes", pending, "amber", "table-pending") }}
      {{ job_table("Accepted / In Progress", accepted, "blue", "table-accepted") }}
      {{ job_table("Completed / Billed", completed, "green", "table-completed") }}
      {{ job_table("Rejected Quotes", rejected, "slate", "table-rejected") }}

    </div>
  </div>
</div>

<script>
function toggleTable(tableId, btn) {
    const tbody = document.getElementById(tableId);
    const hiddenRows = tbody.querySelectorAll('.expanded-row');
    
    let isExpanding = btn.innerText.includes('Show More');
    
    hiddenRows.forEach(row => {
        if (isExpanding) {
            row.classList.remove('hidden');
        } else {
            row.classList.add('hidden');
        }
    });
    
    if (isExpanding) {
        btn.innerHTML = 'Show Less &uarr;';
    } else {
        btn.innerHTML = 'Show More &darr;';
    }
}
</script>
{% endblock %}
'''
    new_content = content[:start_idx] + new_body
    with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
        f.write(new_content)
