import os

# 1. SOA Dashboard
soa_dashboard = """{% extends "layout.html" %}
{% block title %}SOA Management{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-12 px-4 sm:px-6 lg:px-8 font-sans">
  <div class="max-w-4xl mx-auto space-y-8">
    
    <div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">
      {% include "partials/flash_messages.html" %}
      <div class="h-2 bg-purple-500"></div>
      
      <div class="p-8">
        <div class="flex items-center justify-between mb-8 pb-6 border-b border-slate-200">
          <div>
            <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight">SOA Management</h1>
            <p class="text-slate-500 mt-2">Select a property and billing month to manage tenant configurations and generate SOAs.</p>
          </div>
          <a href="{{ url_for('billing_bp.learner_dashboard') }}" class="inline-flex items-center px-4 py-2 bg-slate-100 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-200 transition">
            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
            Back to Dashboard
          </a>
        </div>

        <form method="POST" action="{{ url_for('billing_bp.soa_dashboard') }}" class="space-y-8">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
          
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <!-- Property Selector -->
            <div>
              <label for="property_id" class="block text-sm font-bold text-slate-700 mb-2">Select Property</label>
              <select name="property_id" id="property_id" required class="block w-full pl-3 pr-10 py-3 text-base border-2 border-slate-300 focus:outline-none focus:ring-purple-500 focus:border-purple-500 sm:text-sm rounded-lg bg-white shadow-sm">
                <option value="" disabled selected>-- Choose a Property --</option>
                {% for p in properties %}
                  <option value="{{ p.id }}">{{ p.name }}</option>
                {% endfor %}
              </select>
            </div>

            <!-- Month Selector -->
            <div>
              <label for="month" class="block text-sm font-bold text-slate-700 mb-2">Billing Month</label>
              <input type="month" name="month" id="month" required class="block w-full pl-3 pr-10 py-3 text-base border-2 border-slate-300 focus:outline-none focus:ring-purple-500 focus:border-purple-500 sm:text-sm rounded-lg bg-white shadow-sm" value="{{ current_month }}">
            </div>
          </div>

          <div class="grid grid-cols-1 md:grid-cols-3 gap-6 pt-4 border-t border-slate-200">
            
            <!-- Charge Map Action -->
            <button type="submit" name="action" value="charge_map" class="relative block w-full border-2 border-dashed border-slate-300 rounded-xl p-8 text-center hover:border-purple-500 hover:bg-purple-50 transition duration-150 group">
              <div class="mx-auto flex items-center justify-center h-16 w-16 rounded-full bg-purple-100 text-purple-600 group-hover:bg-purple-200 transition">
                <svg class="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" />
                </svg>
              </div>
              <h3 class="mt-4 text-xl font-bold text-slate-900 group-hover:text-purple-800">SOA Charge Map</h3>
              <p class="mt-2 text-sm text-slate-500 group-hover:text-purple-600">Configure which municipal fixed charges are passed to the tenant.</p>
            </button>

            <!-- Tenants Action -->
            <button type="submit" name="action" value="tenants" class="relative block w-full border-2 border-dashed border-slate-300 rounded-xl p-8 text-center hover:border-blue-500 hover:bg-blue-50 transition duration-150 group">
              <div class="mx-auto flex items-center justify-center h-16 w-16 rounded-full bg-blue-100 text-blue-600 group-hover:bg-blue-200 transition">
                <svg class="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
                </svg>
              </div>
              <h3 class="mt-4 text-xl font-bold text-slate-900 group-hover:text-blue-800">Tenant Management</h3>
              <p class="mt-2 text-sm text-slate-500 group-hover:text-blue-600">Add, edit, or configure details for tenants occupying this property.</p>
            </button>

            <!-- Generate SOA Action -->
            <button type="submit" name="action" value="generate_soa" class="relative block w-full border-2 border-dashed border-slate-300 rounded-xl p-8 text-center hover:border-green-500 hover:bg-green-50 transition duration-150 group">
              <div class="mx-auto flex items-center justify-center h-16 w-16 rounded-full bg-green-100 text-green-600 group-hover:bg-green-200 transition">
                <svg class="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
              <h3 class="mt-4 text-xl font-bold text-slate-900 group-hover:text-green-800">Generate SOAs</h3>
              <p class="mt-2 text-sm text-slate-500 group-hover:text-green-600">Review and generate finalized statements of account (SOAs) for the tenants.</p>
            </button>

          </div>
        </form>

      </div>
    </div>
  </div>
</div>
{% endblock %}
"""

with open('templates/program_billing/soa_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(soa_dashboard)

# 2. SOA Map
soa_map = """{% extends "layout.html" %}
{% block title %}SOA Charge Map{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-12 px-4 sm:px-6 lg:px-8 font-sans">
  <div class="max-w-7xl mx-auto space-y-8">
    
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
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {% for acc in muni_accounts %}
          <div class="bg-slate-50 p-6 rounded-xl border border-slate-200 shadow-sm">
            <h3 class="font-bold text-slate-800 mb-4 border-b pb-2">Municipal Account: {{ acc.account_number or 'Unspecified' }}</h3>
            <form method="POST" action="{{ url_for('billing_bp.update_soa_map') }}">
              <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
              <input type="hidden" name="account_id" value="{{ acc.id }}"/>
              <input type="hidden" name="property_id" value="{{ property.id }}"/>
              <input type="hidden" name="month" value="{{ month }}"/>
              
              <div class="space-y-4">
                <div>
                  <label class="block text-sm font-semibold text-slate-600 mb-1">Rates Charge</label>
                  <select name="rates_charge_to" class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-purple-500 focus:border-purple-500 bg-white">
                    <option value="owner" {% if acc.rates_charge_to == 'owner' %}selected{% endif %}>Owner</option>
                    <option value="tenant" {% if acc.rates_charge_to == 'tenant' %}selected{% endif %}>Tenant</option>
                  </select>
                </div>
                <div>
                  <label class="block text-sm font-semibold text-slate-600 mb-1">Arrears Charge</label>
                  <select name="arrears_charge_to" class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-purple-500 focus:border-purple-500 bg-white">
                    <option value="owner" {% if acc.arrears_charge_to == 'owner' %}selected{% endif %}>Owner</option>
                    <option value="tenant" {% if acc.arrears_charge_to == 'tenant' %}selected{% endif %}>Tenant</option>
                  </select>
                </div>
                <div>
                  <label class="block text-sm font-semibold text-slate-600 mb-1">Arrangements Installment</label>
                  <select name="arrangement_charge_to" class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-purple-500 focus:border-purple-500 bg-white">
                    <option value="owner" {% if acc.arrangement_charge_to == 'owner' %}selected{% endif %}>Owner</option>
                    <option value="tenant" {% if acc.arrangement_charge_to == 'tenant' %}selected{% endif %}>Tenant</option>
                  </select>
                </div>
              </div>
              
              <button type="submit" class="mt-6 w-full bg-purple-600 text-white text-sm font-bold py-2 rounded-lg hover:bg-purple-700 transition shadow-sm">Save Map</button>
            </form>
          </div>
          {% endfor %}
        </div>
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
    f.write(soa_map)

# 3. SOA Tenants
soa_tenants = """{% extends "layout.html" %}
{% block title %}Tenant Management{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-12 px-4 sm:px-6 lg:px-8 font-sans">
  <div class="max-w-7xl mx-auto space-y-8">
    
    {% include "partials/flash_messages.html" %}

    <div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">
      <div class="h-2 bg-blue-500"></div>
      
      <div class="p-8">
        <div class="flex items-center justify-between mb-8 pb-6 border-b border-slate-200">
          <div>
            <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight">Tenant Management</h1>
            <p class="text-slate-500 mt-2">Property: <strong>{{ property.name }}</strong> | Month: <strong>{{ month }}</strong></p>
          </div>
          <div class="flex space-x-3">
            <a href="#" onclick="alert('Add Tenant logic goes here')" class="inline-flex items-center px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition shadow-sm">
              + Add Tenant
            </a>
            <a href="{{ url_for('billing_bp.soa_dashboard') }}" class="inline-flex items-center px-4 py-2 bg-slate-100 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-200 transition">
              <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
              Back to SOA Hub
            </a>
          </div>
        </div>

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
                <a href="#" onclick="if(confirm('Are you sure you want to delete this tenant?')) alert('Delete logic goes here'); return false;" class="inline-flex items-center px-3 py-1.5 bg-red-50 border border-red-200 rounded-md text-sm font-medium text-red-600 hover:bg-red-100 transition">
                  Delete
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
    </div>
  </div>
</div>
{% endblock %}
"""

with open('templates/program_billing/soa_tenants.html', 'w', encoding='utf-8') as f:
    f.write(soa_tenants)

# 4. SOA Generate
soa_generate = """{% extends "layout.html" %}
{% block title %}Generate SOAs{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-12 px-4 sm:px-6 lg:px-8 font-sans">
  <div class="max-w-7xl mx-auto space-y-8">
    
    {% include "partials/flash_messages.html" %}

    <div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">
      <div class="h-2 bg-green-500"></div>
      
      <div class="p-8">
        <div class="flex items-center justify-between mb-8 pb-6 border-b border-slate-200">
          <div>
            <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight">Generate SOAs</h1>
            <p class="text-slate-500 mt-2">Property: <strong>{{ property.name }}</strong> | Month: <strong>{{ month }}</strong></p>
          </div>
          <a href="{{ url_for('billing_bp.soa_dashboard') }}" class="inline-flex items-center px-4 py-2 bg-slate-100 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-200 transition">
            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
            Back to SOA Hub
          </a>
        </div>

        <div class="overflow-x-auto rounded-lg border border-slate-200">
          <table class="w-full text-left border-collapse min-w-max">
            <thead>
              <tr class="bg-slate-50 text-slate-500 text-xs font-semibold uppercase tracking-wider border-b border-slate-200">
                <th class="px-6 py-4">Tenant Name</th>
                <th class="px-6 py-4">Status</th>
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
              <td class="px-6 py-4 text-right space-x-2">
                <a href="{{ url_for('billing_bp.generate_soa', tenant_id=tenant.id, month=month, view='html') }}" target="_blank" class="inline-flex items-center px-3 py-1.5 bg-white border border-slate-300 rounded-md text-sm font-medium text-slate-700 hover:bg-slate-50 transition shadow-sm">
                  Preview HTML
                </a>
                <a href="{{ url_for('billing_bp.generate_soa', tenant_id=tenant.id, month=month) }}" class="inline-flex items-center px-3 py-1.5 bg-green-600 border border-transparent rounded-md text-sm font-medium text-white hover:bg-green-700 transition shadow-sm">
                  Generate PDF
                </a>
              </td>
            </tr>
            {% else %}
            <tr>
              <td colspan="3" class="px-6 py-12 text-center text-slate-500">
                No tenants found for this property.
              </td>
            </tr>
            {% endfor %}
            </tbody>
          </table>
        </div>

      </div>
    </div>
  </div>
</div>
{% endblock %}
"""

with open('templates/program_billing/soa_generate.html', 'w', encoding='utf-8') as f:
    f.write(soa_generate)

print("Created new templates!")
