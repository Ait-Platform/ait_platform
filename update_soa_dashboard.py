new_html = """{% extends "layout.html" %}
{% block title %}SOA Management{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-12 px-4 sm:px-6 lg:px-8 font-sans">
  <div class="max-w-7xl mx-auto space-y-8">
    
    {% include "partials/flash_messages.html" %}

    <div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">
      <div class="h-2 bg-purple-500"></div>
      
      <div class="p-8">
        <div class="flex items-center justify-between mb-8 pb-6 border-b border-slate-200">
          <div>
            <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight">SOA Management</h1>
            <p class="text-slate-500 mt-2">Manage tenant configurations and issue Statements of Account (SOA).</p>
          </div>
          <a href="{{ url_for('billing_bp.learner_dashboard') }}" class="inline-flex items-center px-4 py-2 bg-slate-100 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-200 transition">
            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
            Back to Dashboard
          </a>
        </div>

        <form method="POST" action="{{ url_for('billing_bp.soa_dashboard') }}" class="space-y-8 mb-8">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
          
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <!-- Property Selector -->
            <div>
              <label for="property_id" class="block text-sm font-bold text-slate-700 mb-2">Select Property</label>
              <select name="property_id" id="property_id" required class="block w-full pl-3 pr-10 py-3 text-base border-2 border-slate-300 focus:outline-none focus:ring-purple-500 focus:border-purple-500 sm:text-sm rounded-lg bg-white shadow-sm" onchange="this.form.submit()">
                <option value="" disabled {% if not selected_prop %}selected{% endif %}>-- Choose a Property --</option>
                {% for p in properties %}
                  <option value="{{ p.id }}" {% if selected_prop and selected_prop.id == p.id %}selected{% endif %}>{{ p.name }}</option>
                {% endfor %}
              </select>
            </div>

            <!-- Month Selector -->
            <div>
              <label for="month" class="block text-sm font-bold text-slate-700 mb-2">Billing Month</label>
              <input type="month" name="month" id="month" required class="block w-full pl-3 pr-10 py-3 text-base border-2 border-slate-300 focus:outline-none focus:ring-purple-500 focus:border-purple-500 sm:text-sm rounded-lg bg-white shadow-sm" value="{{ current_month }}" onchange="this.form.submit()">
            </div>
          </div>
        </form>

        {% if selected_prop %}
          
          {% if muni_accounts %}
          <div class="mb-10 p-6 bg-slate-50 rounded-xl border border-slate-200">
            <h2 class="text-xl font-bold text-slate-800 mb-4">SOA Charge Map</h2>
            <p class="text-sm text-slate-500 mb-6">Select whether these municipal charges are billed to the Property Owner or passed through to the Tenant on their SOA.</p>
            
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {% for acc in muni_accounts %}
              <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200">
                <h3 class="font-bold text-slate-700 mb-3 border-b pb-2">Account: {{ acc.account_number or 'Unspecified' }}</h3>
                <form method="POST" action="{{ url_for('billing_bp.update_soa_map') }}">
                  <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                  <input type="hidden" name="account_id" value="{{ acc.id }}"/>
                  <input type="hidden" name="property_id" value="{{ selected_prop.id }}"/>
                  <input type="hidden" name="month" value="{{ current_month }}"/>
                  
                  <div class="space-y-3">
                    <div>
                      <label class="block text-xs font-semibold text-slate-500 mb-1">Rates Charge</label>
                      <select name="rates_charge_to" class="w-full border border-slate-300 rounded px-2 py-1 text-sm focus:ring-purple-500 focus:border-purple-500">
                        <option value="owner" {% if acc.rates_charge_to == 'owner' %}selected{% endif %}>Owner</option>
                        <option value="tenant" {% if acc.rates_charge_to == 'tenant' %}selected{% endif %}>Tenant</option>
                      </select>
                    </div>
                    <div>
                      <label class="block text-xs font-semibold text-slate-500 mb-1">Arrears Charge</label>
                      <select name="arrears_charge_to" class="w-full border border-slate-300 rounded px-2 py-1 text-sm focus:ring-purple-500 focus:border-purple-500">
                        <option value="owner" {% if acc.arrears_charge_to == 'owner' %}selected{% endif %}>Owner</option>
                        <option value="tenant" {% if acc.arrears_charge_to == 'tenant' %}selected{% endif %}>Tenant</option>
                      </select>
                    </div>
                    <div>
                      <label class="block text-xs font-semibold text-slate-500 mb-1">Arrangements Installment</label>
                      <select name="arrangement_charge_to" class="w-full border border-slate-300 rounded px-2 py-1 text-sm focus:ring-purple-500 focus:border-purple-500">
                        <option value="owner" {% if acc.arrangement_charge_to == 'owner' %}selected{% endif %}>Owner</option>
                        <option value="tenant" {% if acc.arrangement_charge_to == 'tenant' %}selected{% endif %}>Tenant</option>
                      </select>
                    </div>
                  </div>
                  
                  <button type="submit" class="mt-4 w-full bg-slate-800 text-white text-xs font-bold py-2 rounded hover:bg-slate-700 transition">Save Map</button>
                </form>
              </div>
              {% endfor %}
            </div>
          </div>
          {% endif %}

          <div class="mb-10">
            <h2 class="text-xl font-bold text-slate-800 mb-4">Tenants for {{ selected_prop.name }}</h2>
            
            <div class="overflow-x-auto rounded-lg border border-slate-200">
              <table class="w-full text-left border-collapse min-w-max">
                <thead>
                  <tr class="bg-slate-50 text-slate-500 text-xs font-semibold uppercase tracking-wider border-b border-slate-200">
                    <th class="px-6 py-4">Tenant Name</th>
                    <th class="px-6 py-4">Status</th>
                    <th class="px-6 py-4">Started</th>
                    <th class="px-6 py-4">Terminated</th>
                    <th class="px-6 py-4 text-right">Actions</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-slate-100">
                {% for tenant in tenants %}
                <tr class="hover:bg-slate-50 transition duration-150">
                  <td class="px-6 py-4 font-bold text-slate-800">{{ tenant.name }}</td>
                  <td class="px-6 py-4">
                    {% if tenant.is_active %}
                      <span class="px-2.5 py-1 text-xs font-bold uppercase rounded-full bg-green-100 text-green-800 border border-green-200">Active</span>
                    {% else %}
                      <span class="px-2.5 py-1 text-xs font-bold uppercase rounded-full bg-slate-100 text-slate-600 border border-slate-200">Inactive</span>
                    {% endif %}
                  </td>
                  <td class="px-6 py-4 text-sm text-slate-600">{{ tenant.date_started.strftime('%Y-%m-%d') if tenant.date_started else '-' }}</td>
                  <td class="px-6 py-4 text-sm text-slate-600">{{ tenant.date_terminated.strftime('%Y-%m-%d') if tenant.date_terminated else '-' }}</td>
                  <td class="px-6 py-4 text-right space-x-2">
                    <a href="{{ url_for('billing_bp.edit_tenant_soa', tenant_id=tenant.id) }}" class="inline-flex items-center px-3 py-1.5 bg-white border border-slate-300 rounded-md text-sm font-medium text-slate-700 hover:bg-slate-50 transition">
                      Configure
                    </a>
                    <a href="{{ url_for('billing_bp.generate_soa', tenant_id=tenant.id, month=current_month) }}" class="inline-flex items-center px-3 py-1.5 bg-purple-600 border border-transparent rounded-md text-sm font-medium text-white hover:bg-purple-700 transition shadow-sm">
                      Generate SOA
                    </a>
                  </td>
                </tr>
                {% else %}
                <tr>
                  <td colspan="5" class="px-6 py-12 text-center text-slate-500">
                    No tenants found for this property.
                  </td>
                </tr>
                {% endfor %}
                </tbody>
              </table>
            </div>
          </div>
        {% else %}
          <div class="text-center py-12 bg-slate-50 rounded-xl border-2 border-dashed border-slate-300">
            <svg class="mx-auto h-12 w-12 text-slate-400 mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
            </svg>
            <h3 class="text-lg font-medium text-slate-900">No Property Selected</h3>
            <p class="mt-1 text-sm text-slate-500">Please select a property and billing month above to view the SOA Dashboard.</p>
          </div>
        {% endif %}

      </div>
    </div>
  </div>
</div>
{% endblock %}
"""

with open('templates/program_billing/soa_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(new_html)

print("Updated soa_dashboard.html")
