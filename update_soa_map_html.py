import os

html = """{% extends "layout.html" %}
{% block title %}SOA Charge Map{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-12 px-4 sm:px-6 lg:px-8 font-sans">
  <div class="max-w-4xl mx-auto space-y-8">
    
    {% include "partials/flash_messages.html" %}

    <div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">
      <div class="h-2 bg-purple-500"></div>
      
      <div class="p-8">
        <div class="flex items-center justify-between mb-8 pb-6 border-b border-slate-200">
          <div>
            <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight">SOA Charge Map</h1>
            <p class="text-slate-500 mt-2">Property: <strong>{{ property.name }}</strong> | Month: <strong>{{ month }}</strong></p>
          </div>
          <a href="{{ url_for('billing_bp.soa_dashboard') }}" class="inline-flex items-center px-4 py-2 bg-slate-100 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-200 transition">
            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
            Back to SOA Hub
          </a>
        </div>

        {% if muni_accounts %}
        <form method="GET" action="{{ url_for('billing_bp.soa_map_view', property_id=property.id, month=month) }}" class="mb-8">
          <div>
            <label for="account_id" class="block text-sm font-bold text-slate-700 mb-2">Select Municipal Account</label>
            <select name="account_id" id="account_id" required class="block w-full pl-3 pr-10 py-3 text-base border-2 border-slate-300 focus:outline-none focus:ring-purple-500 focus:border-purple-500 sm:text-sm rounded-lg bg-white shadow-sm" onchange="this.form.submit()">
              <option value="" disabled {% if not selected_account %}selected{% endif %}>-- Choose an Account --</option>
              {% for acc in muni_accounts %}
                <option value="{{ acc.id }}" {% if selected_account and selected_account.id == acc.id %}selected{% endif %}>{{ acc.account_number or ('Account ID: ' ~ acc.id|string) }}</option>
              {% endfor %}
            </select>
          </div>
        </form>

        {% if selected_account %}
        <div class="bg-slate-50 p-6 rounded-xl border border-slate-200 shadow-sm">
          <div class="mb-6 border-b pb-4">
            <h3 class="font-bold text-slate-800 text-lg">Charge Mapping for Account: {{ selected_account.account_number or 'Unspecified' }}</h3>
            <p class="text-sm text-slate-500 mt-1">
              Verify the amounts below and select who is responsible. 
              If the stored value is missing or incorrect, you can adjust it here, or return to the Property Configuration Wizard to update the master record.
            </p>
          </div>
          
          <form method="POST" action="{{ url_for('billing_bp.update_soa_map') }}">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
            <input type="hidden" name="account_id" value="{{ selected_account.id }}"/>
            <input type="hidden" name="property_id" value="{{ property.id }}"/>
            <input type="hidden" name="month" value="{{ month }}"/>
            
            <div class="space-y-6">
              
              <!-- Rates Section -->
              <div class="bg-white p-4 rounded-lg border border-slate-200">
                <div class="flex items-center justify-between mb-3">
                  <label class="block text-sm font-bold text-slate-700">Rates & SRA Charge</label>
                  {% set default_rates = selected_account.rates_amount if selected_account.rates_amount else ((selected_account.rates_general_monthly or 0) + (selected_account.rates_sra_monthly or 0)) %}
                  {% if not default_rates %}
                    <span class="text-xs text-orange-600 bg-orange-50 px-2 py-1 rounded-full font-medium">Missing Data in Wizard</span>
                  {% endif %}
                </div>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <label class="block text-xs text-slate-500 mb-1">Amount to Charge (R)</label>
                    <input type="number" step="0.01" name="rates_amount" value="{{ '%.2f'|format(default_rates) }}" class="w-full border-2 border-slate-300 rounded-lg px-3 py-2 focus:ring-purple-500 focus:border-purple-500">
                  </div>
                  <div>
                    <label class="block text-xs text-slate-500 mb-1">Charge To</label>
                    <select name="rates_charge_to" class="w-full border-2 border-slate-300 rounded-lg px-3 py-2 focus:ring-purple-500 focus:border-purple-500 bg-white">
                      <option value="owner" {% if selected_account.rates_charge_to == 'owner' %}selected{% endif %}>Owner</option>
                      <option value="tenant" {% if selected_account.rates_charge_to == 'tenant' %}selected{% endif %}>Tenant</option>
                    </select>
                  </div>
                </div>
              </div>

              <!-- Arrears Section -->
              <div class="bg-white p-4 rounded-lg border border-slate-200">
                <div class="flex items-center justify-between mb-3">
                  <label class="block text-sm font-bold text-slate-700">Arrears Amount</label>
                  {% if not selected_account.arrears_amount %}
                    <span class="text-xs text-orange-600 bg-orange-50 px-2 py-1 rounded-full font-medium">Missing Data in Wizard</span>
                  {% endif %}
                </div>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <label class="block text-xs text-slate-500 mb-1">Amount to Charge (R)</label>
                    <input type="number" step="0.01" name="arrears_amount" value="{{ '%.2f'|format(selected_account.arrears_amount or 0) }}" class="w-full border-2 border-slate-300 rounded-lg px-3 py-2 focus:ring-purple-500 focus:border-purple-500">
                  </div>
                  <div>
                    <label class="block text-xs text-slate-500 mb-1">Charge To</label>
                    <select name="arrears_charge_to" class="w-full border-2 border-slate-300 rounded-lg px-3 py-2 focus:ring-purple-500 focus:border-purple-500 bg-white">
                      <option value="owner" {% if selected_account.arrears_charge_to == 'owner' %}selected{% endif %}>Owner</option>
                      <option value="tenant" {% if selected_account.arrears_charge_to == 'tenant' %}selected{% endif %}>Tenant</option>
                    </select>
                  </div>
                </div>
              </div>

              <!-- Arrangement Section -->
              <div class="bg-white p-4 rounded-lg border border-slate-200">
                <div class="flex items-center justify-between mb-3">
                  <label class="block text-sm font-bold text-slate-700">Credit Arrangement Installment</label>
                  {% if not selected_account.ca_installment_amount %}
                    <span class="text-xs text-orange-600 bg-orange-50 px-2 py-1 rounded-full font-medium">Missing Data in Wizard</span>
                  {% endif %}
                </div>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <label class="block text-xs text-slate-500 mb-1">Amount to Charge (R)</label>
                    <input type="number" step="0.01" name="ca_installment_amount" value="{{ '%.2f'|format(selected_account.ca_installment_amount or 0) }}" class="w-full border-2 border-slate-300 rounded-lg px-3 py-2 focus:ring-purple-500 focus:border-purple-500">
                  </div>
                  <div>
                    <label class="block text-xs text-slate-500 mb-1">Charge To</label>
                    <select name="arrangement_charge_to" class="w-full border-2 border-slate-300 rounded-lg px-3 py-2 focus:ring-purple-500 focus:border-purple-500 bg-white">
                      <option value="owner" {% if selected_account.arrangement_charge_to == 'owner' %}selected{% endif %}>Owner</option>
                      <option value="tenant" {% if selected_account.arrangement_charge_to == 'tenant' %}selected{% endif %}>Tenant</option>
                    </select>
                  </div>
                </div>
              </div>

            </div>
            
            <div class="mt-8 pt-4 border-t border-slate-200">
              <button type="submit" class="w-full bg-purple-600 text-white text-lg font-bold py-3 rounded-xl hover:bg-purple-700 transition shadow-md">
                Record Mapping
              </button>
            </div>
          </form>
        </div>
        {% else %}
        <div class="text-center py-12 bg-slate-50 rounded-xl border-2 border-dashed border-slate-300">
          <p class="text-slate-500">Please select a municipal account from the dropdown above.</p>
        </div>
        {% endif %}
        
        {% else %}
        <div class="text-center py-12 bg-slate-50 rounded-xl border-2 border-dashed border-slate-300">
          <p class="text-slate-500">No municipal accounts found for this property.</p>
        </div>
        {% endif %}

      </div>
    </div>
  </div>
</div>
{% endblock %}
"""

with open('templates/program_billing/soa_map.html', 'w', encoding='utf-8') as f:
    f.write(html)

print("Updated soa_map.html UI")
