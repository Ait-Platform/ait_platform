with open('templates/program_mechanic/debtors_schedule.html', 'w', encoding='utf-8') as f:
    f.write('''{% extends 'layout.html' %}
{% block title %}Debtors Schedule{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-10 px-4 flex flex-col items-center">
  <div class="w-full max-w-5xl bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
    
    <div class="h-2 w-full bg-slate-700"></div>

    <div class="px-6 pt-6 pb-2">
      {% include "partials/flash_messages.html" %}
    </div>

    <!-- First Row: Title & Back Button -->
    <div class="flex flex-col md:flex-row md:items-center justify-between border-b border-slate-100 px-6 pb-4 gap-4">
      <div>
        <h1 class="text-2xl md:text-3xl font-bold text-slate-900">Debtors Schedule for {{ shop.business_name if shop else 'Your Shop' }}</h1>
        <p class="mt-1 text-sm text-slate-500 font-semibold uppercase tracking-wide">
          {% if start_date and end_date %}
            For the period {{ start_date }} to {{ end_date }}
          {% elif start_date %}
            For the period since {{ start_date }}
          {% elif end_date %}
            For the period up to {{ end_date }}
          {% else %}
            All Time
          {% endif %}
        </p>
      </div>
      <div class="flex items-center gap-3">
        <button onclick="window.print()" class="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center text-slate-600 hover:bg-slate-200 hover:text-slate-800 transition shadow-sm" title="Print Schedule">
          <i class="fas fa-print"></i>
        </button>
        <a href="{{ url_for('mechanic_bp.client_accounts') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
          <span>&larr;</span><span>Client Accounts</span>
        </a>
      </div>
    </div>
    
    <div class="px-6 py-4 bg-indigo-50 border-b border-slate-100 flex flex-col md:flex-row justify-end items-center gap-4">
        <div class="bg-white p-3 rounded-lg border border-indigo-100 shadow-sm text-right">
            <div class="text-xs font-bold text-slate-500 uppercase">Total Outstanding (Period)</div>
            <div class="text-2xl font-bold text-red-600">R {{ "{:,.2f}".format(total_owed) }}</div>
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
                <th scope="col" class="px-6 py-3 text-right text-xs font-bold text-slate-500 uppercase tracking-wider">Balance</th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-slate-100">
              {% for d in debtors %}
              <tr class="hover:bg-slate-50 transition">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-bold text-slate-900">{{ d.name }}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{{ d.email or d.phone or 'N/A' }}</td>
                <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-bold {% if d.current_balance > 0 %}text-red-600{% else %}text-green-600{% endif %}">
                  R {{ "%.2f"|format(d.current_balance) }}
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
          <p class="text-slate-500 mt-1">When you create job cards, your clients will appear here.</p>
        </div>
      {% endif %}
    </div>
  </div>
</div>
{% endblock %}''')
