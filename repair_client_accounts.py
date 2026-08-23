with open('templates/program_mechanic/client_accounts.html', 'w', encoding='utf-8') as f:
    f.write('''{% extends 'layout.html' %}
{% block title %}Client Accounts - ProTrade{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-10 px-4 flex flex-col items-center">
  <div class="w-full max-w-6xl bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
    
    <div class="h-2 w-full bg-slate-800"></div>

    <div class="px-6 pt-6 pb-2">
      {% include "partials/flash_messages.html" %}
    </div>

    <!-- Row 1: Title & Back Button -->
    <div class="flex flex-col md:flex-row md:items-center justify-between border-b border-slate-100 px-6 pb-4 gap-4">
      <div>
        <h1 class="text-2xl md:text-3xl font-bold text-slate-900">Client Accounts (Ledger)</h1>
        <p class="mt-1 text-sm text-slate-500">Manage your clients and their individual ledgers.</p>
      </div>
      <div class="flex justify-end">
        <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm outline-none focus:ring-2 focus:ring-indigo-500">
          <span>&larr;</span><span>Back</span>
        </a>
      </div>
    </div>
    
    <!-- Row 2: Other Buttons right aligned -->
    <div class="px-6 py-4 flex justify-end">
        <button onclick="document.getElementById('scheduleModal').classList.remove('hidden'); document.getElementById('start_date_input').focus();" class="inline-flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 transition shadow-sm outline-none focus:ring-2 focus:ring-indigo-500">
          <i class="fas fa-file-invoice-dollar"></i><span>Debtors Schedule</span>
        </button>
    </div>
    
    <div class="p-6 pt-0">
      {% if debtors %}
        <div class="border border-slate-200 rounded-xl overflow-x-auto shadow-sm bg-white">
          <table class="min-w-full divide-y divide-slate-200">
            <thead class="bg-slate-50">
              <tr>
                <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Client</th>
                <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Contact</th>
                <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Balance</th>
                <th scope="col" class="px-6 py-3 text-right text-xs font-bold text-slate-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-slate-100">
              {% for d in debtors %}
              <tr class="hover:bg-slate-50 transition">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-bold text-slate-900">{{ d.name }}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{{ d.email or d.phone or 'N/A' }}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-bold {% if d.current_balance > 0 %}text-red-600{% else %}text-green-600{% endif %}">
                  R {{ "%.2f"|format(d.current_balance) }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                  <a href="{{ url_for('mechanic_bp.client_ledger', debtor_id=d.id) }}" class="text-white bg-slate-800 hover:bg-slate-900 px-3 py-1.5 rounded-md transition shadow-sm font-semibold">
                    View Ledger &rarr;
                  </a>
                </td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      {% else %}
        <div class="text-center py-12 border border-slate-200 rounded-xl">
          <i class="fas fa-users text-4xl text-slate-300 mb-3"></i>
          <h3 class="text-lg font-bold text-slate-700">No Clients Found</h3>
          <p class="text-slate-500 mt-1">Clients will automatically appear here when you create Job Cards for them.</p>
        </div>
      {% endif %}
    </div>
  </div>
</div>

<!-- Generate Schedule Modal -->
<div id="scheduleModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-slate-900 bg-opacity-50">
  <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden">
    <div class="bg-indigo-600 px-6 py-4 flex justify-between items-center">
      <h3 class="text-lg font-bold text-white">Generate Debtors Schedule</h3>
      <button onclick="document.getElementById('scheduleModal').classList.add('hidden')" class="text-indigo-200 hover:text-white transition outline-none focus:ring-2 focus:ring-white">
        <i class="fas fa-times"></i>
      </button>
    </div>
    <form action="{{ url_for('mechanic_bp.generate_debtors_schedule') }}" method="POST" class="p-6">
      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
      <p class="text-sm text-slate-600 mb-4">Generate a snapshot balance sheet of all your debtors.</p>
      
      <div class="space-y-4">
        <div>
          <label class="block text-sm font-bold text-slate-700 mb-1">Start Date (Optional)</label>
          <input type="date" id="start_date_input" name="start_date" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
        </div>
        <div>
          <label class="block text-sm font-bold text-slate-700 mb-1">End Date (Optional)</label>
          <input type="date" name="end_date" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
        </div>
      </div>
      
      <div class="mt-6 flex justify-end gap-3">
        <button type="button" onclick="document.getElementById('scheduleModal').classList.add('hidden')" class="px-4 py-2 text-sm font-bold text-slate-600 hover:text-slate-900 outline-none focus:ring-2 focus:ring-indigo-500 rounded-lg">Cancel</button>
        <button type="submit" class="px-4 py-2 bg-indigo-600 text-white text-sm font-bold rounded-lg hover:bg-indigo-700 shadow-sm transition outline-none focus:ring-2 focus:ring-indigo-500">Generate Schedule</button>
      </div>
    </form>
  </div>
</div>
{% endblock %}
''')
