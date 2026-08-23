import re

with open('templates/program_mechanic/client_ledger.html', 'r', encoding='utf-8') as f:
    content = f.read()

job_cards_html = '''
  <!-- Client Job Cards -->
  <div class="mt-8 bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden mb-8">
    <div class="bg-slate-50 px-6 py-4 border-b border-slate-200 flex justify-between items-center">
      <h2 class="text-lg font-bold text-slate-800">Client Job Cards & Tax Invoices</h2>
    </div>
    <div class="overflow-x-auto">
      <table class="w-full text-left border-collapse">
        <thead>
          <tr class="bg-white border-b border-slate-200 text-xs uppercase text-slate-500 tracking-wider">
            <th class="px-6 py-3 font-bold">Date</th>
            <th class="px-6 py-3 font-bold">Job #</th>
            <th class="px-6 py-3 font-bold">Vehicle</th>
            <th class="px-6 py-3 font-bold">Status</th>
            <th class="px-6 py-3 font-bold text-right">Actions</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100">
          {% for j in job_cards %}
          <tr class="hover:bg-slate-50 transition">
            <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-700">{{ j.created_at.strftime('%Y-%m-%d') }}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm font-bold text-indigo-600">JOB-{{ j.job_number }}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{{ j.vehicle.make }} {{ j.vehicle.model }}</td>
            <td class="px-6 py-4 whitespace-nowrap">
              <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold 
                {% if j.status == 'Billed' %}bg-green-100 text-green-800
                {% elif j.status in ['Approved', 'Awaiting Deposit'] %}bg-blue-100 text-blue-800
                {% elif j.status == 'Rejected' %}bg-slate-100 text-slate-800
                {% else %}bg-amber-100 text-amber-800{% endif %}">
                {{ j.status }}
              </span>
            </td>
            <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
              <a href="{{ url_for('mechanic_bp.job_card_detail', id=j.id) }}" class="text-indigo-600 hover:text-indigo-900 font-bold mr-3" title="Open Job Card Hub">
                <i class="fas fa-external-link-alt"></i> Open Hub
              </a>
              {% if j.status not in ['Quote', 'Rejected'] %}
              <a href="{{ url_for('mechanic_bp.download_document', id=j.id) }}" class="text-slate-500 hover:text-slate-800 font-bold mr-3" title="Download Tax Invoice">
                <i class="fas fa-file-pdf"></i> Tax Invoice
              </a>
              <a href="{{ url_for('mechanic_bp.email_document', id=j.id) }}" class="text-blue-500 hover:text-blue-800 font-bold" title="Email Tax Invoice">
                <i class="fas fa-paper-plane"></i> Email
              </a>
              {% else %}
              <span class="text-slate-400 italic text-xs">Waiting for Acceptance</span>
              {% endif %}
            </td>
          </tr>
          {% else %}
          <tr>
            <td colspan="5" class="px-6 py-8 text-center text-slate-500 text-sm">No job cards found for this client.</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>

<!-- Record Payment Modal -->
'''

content = content.replace('<!-- Record Payment Modal -->', job_cards_html)

with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content)
