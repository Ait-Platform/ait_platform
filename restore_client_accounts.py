import os

with open('templates/program_mechanic/client_accounts.html', 'w', encoding='utf-8') as f:
    f.write('''{% extends 'layout.html' %}
{% block title %}Client Accounts - ProTrade{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-10 px-4 flex flex-col items-center">
  <div class="w-full max-w-6xl bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
    
    <div class="h-2 w-full bg-slate-700"></div>

    <div class="px-6 pt-6 pb-2">
      {% include "partials/flash_messages.html" %}
    </div>

    <!-- First Row: Title & Back Button -->
    <div class="flex flex-col md:flex-row md:items-center justify-between border-b border-slate-100 px-6 pb-4 gap-4">
      <div>
        <h1 class="text-2xl md:text-3xl font-bold text-slate-900">Client Accounts (Ledger)</h1>
        <p class="mt-1 text-sm text-slate-500">Manage your clients and their individual ledgers.</p>
      </div>
      <div class="flex items-center gap-3">
        <a href="{{ url_for('mechanic_bp.debtors_schedule_options') }}" class="inline-flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 transition shadow-sm">
          <i class="fas fa-file-invoice-dollar"></i><span>Debtors Schedule (10 Tokens)</span>
        </a>
        <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
          <span>&larr;</span><span>Dashboard</span>
        </a>
      </div>
    </div>
    
    <div class="p-6">
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
        <div class="text-center py-12">
          <i class="fas fa-users text-4xl text-slate-300 mb-3"></i>
          <h3 class="text-lg font-bold text-slate-700">No Clients Found</h3>
          <p class="text-slate-500 mt-1">Clients will automatically appear here when you create Job Cards for them.</p>
        </div>
      {% endif %}
    </div>
  </div>
</div>
{% endblock %}''')
