html_content = """{% extends "layout.html" %}

{% block title %}Manual Bill Capture (Account Builder){% endblock %}

{% block content %}
<div class="max-w-6xl mx-auto px-4 py-8">
  <form method="POST" action="{{ url_for('billing_bp.manual_capture') }}" id="manualCaptureForm">
    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
    <input type="hidden" name="property_id" value="{{ property.id }}">
    
    <!-- JSON payload to send accounts and meters dynamically -->
    <input type="hidden" name="payload_json" id="payloadJson">

    <div class="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
      <!-- Color strip on top -->
      <div class="h-2 bg-emerald-500"></div>

      <div class="p-6 md:p-8 space-y-6">
        <!-- Header -->
        <div class="flex items-center justify-between border-b border-slate-100 pb-4">
          <div>
            <h1 class="text-2xl md:text-3xl font-extrabold text-slate-900 tracking-tight">Manual Bill Capture</h1>
            <p class="text-sm text-slate-500 mt-1">Property: <span class="font-semibold text-slate-700">{{ property.name }}</span> | Expected Statements: <span class="font-bold text-blue-600">{{ property.expected_bills }}</span></p>
          </div>
          <a href="{{ url_for('billing_bp.learner_dashboard') }}" class="inline-flex items-center text-sm font-medium text-slate-500 hover:text-slate-800 transition bg-white hover:bg-slate-50 px-4 py-2 rounded-lg border border-slate-200 shadow-sm">
            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
            Back to Dashboard
          </a>
        </div>

        {% include "partials/flash_messages.html" ignore missing %}

        <div class="bg-blue-50 border border-blue-200 rounded-xl p-5 shadow-sm">
          <div class="flex">
            <svg class="w-6 h-6 text-blue-600 mr-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
            <div>
              <h3 class="text-sm font-bold text-blue-900 uppercase tracking-wide">Account Builder</h3>
              <p class="text-sm text-blue-800 mt-1">Because you expect <strong>{{ property.expected_bills }}</strong> statements from the municipality, you should build <strong>{{ property.expected_bills }}</strong> distinct Municipal Accounts below. For each account, enter its unique Owner, Rates, and Arrears, and then add the specific Meters that belong to it.</p>
            </div>
          </div>
        </div>

        <div class="flex items-center justify-between mt-8 mb-4">
          <h2 class="text-xl font-bold text-slate-800">Municipal Accounts</h2>
          <div class="text-sm font-semibold px-3 py-1 bg-slate-100 rounded-full text-slate-600 border border-slate-200 shadow-inner">
            <span id="accountCountDisplay">0</span> / {{ property.expected_bills }} Created
          </div>
        </div>

        <!-- Accounts Container -->
        <div id="accountsContainer" class="space-y-8">
          <!-- Accounts will be appended here by JS -->
        </div>

        <div class="pt-6">
          <button type="button" onclick="addAccount()" class="w-full bg-slate-50 hover:bg-slate-100 text-slate-700 font-bold py-4 px-4 rounded-xl border-2 border-dashed border-slate-300 transition flex justify-center items-center group">
            <svg class="w-6 h-6 mr-2 text-slate-400 group-hover:text-emerald-500 transition" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6v6m0 0v6m0-6h6m-6 0H6"></path></svg>
            Add Municipal Account Block
          </button>
        </div>

        <div class="pt-8 border-t border-slate-100 flex justify-end">
          <button type="submit" onclick="preparePayload()" class="bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-3 px-8 rounded-xl shadow-md transition transform hover:-translate-y-0.5">
            Save All Accounts &amp; Continue
          </button>
        </div>

      </div>
    </div>
  </form>
</div>

<!-- Template: Account Block -->
<template id="accountTemplate">
  <div class="account-block bg-white rounded-xl shadow-sm border-2 border-slate-200 overflow-hidden relative transition hover:border-blue-300">
    <div class="bg-slate-50 px-6 py-4 border-b border-slate-200 flex justify-between items-center">
      <h3 class="font-bold text-lg text-slate-800 flex items-center">
        <svg class="w-5 h-5 text-blue-500 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4"></path></svg>
        Municipal Account Block
      </h3>
      <button type="button" onclick="removeAccount(this)" class="text-red-400 hover:text-red-600 font-bold text-sm bg-white px-2 py-1 rounded border border-red-200 hover:bg-red-50 transition">
        Remove Account
      </button>
    </div>
    
    <div class="p-6">
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
        
        <!-- Left: Account Details & Financials -->
        <div class="space-y-6">
          <div class="space-y-4">
            <h4 class="text-sm font-bold text-slate-800 uppercase tracking-wide border-b pb-1">1. Ownership & Account</h4>
            <div>
              <label class="block text-xs font-semibold text-slate-700 mb-1">Owner Name</label>
              <input type="text" class="acc-owner w-full rounded border-2 border-slate-300 px-3 py-1.5 text-sm outline-none focus:border-blue-500" required>
            </div>
            <div>
              <label class="block text-xs font-semibold text-slate-700 mb-1">Unit Address</label>
              <input type="text" class="acc-address w-full rounded border-2 border-slate-300 px-3 py-1.5 text-sm outline-none focus:border-blue-500" required>
            </div>
            <div>
              <label class="block text-xs font-bold text-blue-900 mb-1">Municipal Account Number</label>
              <input type="text" class="acc-number w-full rounded border-2 border-blue-400 px-3 py-1.5 text-sm outline-none focus:border-blue-600 bg-blue-50" required placeholder="e.g. 701...">
            </div>
          </div>

          <div class="space-y-4">
            <h4 class="text-sm font-bold text-slate-800 uppercase tracking-wide border-b pb-1">2. Rates & Valuation</h4>
            <div class="grid grid-cols-2 gap-3">
              <div>
                <label class="block text-xs font-semibold text-slate-700 mb-1">Property Valuation</label>
                <input type="number" step="0.01" class="acc-val w-full rounded border-2 border-slate-300 px-3 py-1.5 text-sm outline-none focus:border-emerald-500" placeholder="0.00">
              </div>
              <div>
                <label class="block text-xs font-semibold text-slate-700 mb-1">Monthly Rates Charge</label>
                <input type="number" step="0.01" class="acc-rates w-full rounded border-2 border-slate-300 px-3 py-1.5 text-sm outline-none focus:border-emerald-500" placeholder="0.00">
              </div>
            </div>
          </div>

          <div class="space-y-4">
            <h4 class="text-sm font-bold text-slate-800 uppercase tracking-wide border-b pb-1">3. Arrears Arrangement</h4>
            <div class="bg-amber-50 p-4 rounded border border-amber-200">
              <div class="grid grid-cols-2 gap-3">
                <div>
                  <label class="block text-[11px] font-bold text-amber-900 mb-1">Monthly Payment (R)</label>
                  <input type="number" step="0.01" class="acc-arr-amount w-full rounded border border-amber-300 px-2 py-1.5 text-sm outline-none focus:border-amber-500" placeholder="0.00">
                </div>
                <div>
                  <label class="block text-[11px] font-bold text-amber-900 mb-1">Term (Months)</label>
                  <input type="number" class="acc-arr-dur w-full rounded border border-amber-300 px-2 py-1.5 text-sm outline-none focus:border-amber-500" placeholder="0">
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Right: Meters Attached to this Account -->
        <div class="space-y-4">
          <h4 class="text-sm font-bold text-slate-800 uppercase tracking-wide border-b pb-1">4. Meters Attached</h4>
          
          <div class="account-meters-container space-y-3 min-h-[100px] p-2 bg-slate-50 border-2 border-dashed border-slate-200 rounded-lg">
            <!-- Meters will be injected here -->
          </div>
          
          <div class="flex space-x-2">
            <button type="button" onclick="addMeter(this, 'water')" class="flex-1 bg-sky-100 hover:bg-sky-200 text-sky-800 font-bold py-2 px-3 rounded border border-sky-300 transition text-xs flex justify-center items-center">
              + Water Meter
            </button>
            <button type="button" onclick="addMeter(this, 'electricity')" class="flex-1 bg-indigo-100 hover:bg-indigo-200 text-indigo-800 font-bold py-2 px-3 rounded border border-indigo-300 transition text-xs flex justify-center items-center">
              + Elec Meter
            </button>
          </div>
        </div>

      </div>
    </div>
  </div>
</template>

<!-- Template: Meter Card -->
<template id="meterTemplate">
  <div class="meter-card relative p-3 rounded bg-white border border-slate-300 shadow-sm transition">
    <button type="button" onclick="removeMeter(this)" class="absolute top-2 right-2 text-red-400 hover:text-red-600">
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
    </button>
    <div class="flex items-center mb-2">
      <div class="meter-icon mr-1.5"></div>
      <h5 class="font-bold text-sm meter-title">Meter</h5>
    </div>
    
    <div class="space-y-3">
      <div>
        <label class="block text-[10px] uppercase font-bold text-slate-500 mb-1">Meter Number</label>
        <input type="text" class="meter-number w-full rounded border border-slate-300 px-2 py-1 text-sm outline-none focus:border-blue-500" required>
      </div>
      <div class="grid grid-cols-2 gap-2">
        <div>
          <label class="block text-[10px] uppercase font-bold text-slate-500 mb-1">Assignment</label>
          <select class="meter-assignment w-full rounded border border-slate-300 px-1 py-1 text-xs outline-none focus:border-blue-500 bg-white">
            <option value="linked">Linked</option>
            <option value="bulk">Bulk</option>
          </select>
        </div>
        <div>
          <label class="block text-[10px] uppercase font-bold text-slate-500 mb-1">Status</label>
          <select class="meter-status w-full rounded border border-slate-300 px-1 py-1 text-xs outline-none focus:border-blue-500 bg-white">
            <option value="active">Active (Normal)</option>
            <option value="stolen">Stolen (On Bill)</option>
            <option value="new_physical">New (Physical)</option>
          </select>
        </div>
      </div>
      
      <div class="grid grid-cols-2 gap-2 bg-slate-50 p-2 rounded border border-slate-200">
        <div>
          <label class="block text-[10px] font-bold text-slate-600 mb-0.5">Prev Date</label>
          <input type="date" class="meter-prev-date w-full rounded border px-1 py-0.5 text-xs outline-none focus:border-emerald-500" required>
        </div>
        <div>
          <label class="block text-[10px] font-bold text-slate-600 mb-0.5">Prev Read</label>
          <input type="number" step="0.01" class="meter-prev-read w-full rounded border px-1 py-0.5 text-xs outline-none focus:border-emerald-500" required>
        </div>
        <div>
          <label class="block text-[10px] font-bold text-slate-600 mb-0.5">Curr Date</label>
          <input type="date" class="meter-curr-date w-full rounded border px-1 py-0.5 text-xs outline-none focus:border-emerald-500" required>
        </div>
        <div>
          <label class="block text-[10px] font-bold text-slate-600 mb-0.5">Curr Read</label>
          <input type="number" step="0.01" class="meter-curr-read w-full rounded border px-1 py-0.5 text-xs outline-none focus:border-emerald-500" required>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
  const maxAccounts = {{ property.expected_bills }};
  let accountCount = 0;

  function addAccount() {
    if (accountCount >= maxAccounts) {
      alert(`You have reached the expected number of statements (${maxAccounts}). To add more, please edit your property setup on the dashboard.`);
      return;
    }
    const template = document.getElementById('accountTemplate').content.cloneNode(true);
    // Unique ID for the account block to help tracking
    template.querySelector('.account-block').dataset.accId = 'acc_' + Date.now();
    document.getElementById('accountsContainer').appendChild(template);
    
    accountCount++;
    document.getElementById('accountCountDisplay').textContent = accountCount;
  }

  function removeAccount(btn) {
    btn.closest('.account-block').remove();
    accountCount--;
    document.getElementById('accountCountDisplay').textContent = accountCount;
  }

  function addMeter(btn, type) {
    const container = btn.closest('.space-y-4').querySelector('.account-meters-container');
    const template = document.getElementById('meterTemplate').content.cloneNode(true);
    const card = template.querySelector('.meter-card');
    
    if (type === 'water') {
      card.classList.add('border-sky-300');
      card.dataset.type = 'water';
      card.querySelector('.meter-title').textContent = 'Water Meter';
      card.querySelector('.meter-title').classList.add('text-sky-800');
      card.querySelector('.meter-icon').innerHTML = '<svg class="w-4 h-4 text-sky-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"></path></svg>';
    } else {
      card.classList.add('border-indigo-300');
      card.dataset.type = 'electricity';
      card.querySelector('.meter-title').textContent = 'Elec Meter';
      card.querySelector('.meter-title').classList.add('text-indigo-800');
      card.querySelector('.meter-icon').innerHTML = '<svg class="w-4 h-4 text-indigo-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>';
    }

    container.appendChild(card);
  }

  function removeMeter(btn) {
    btn.closest('.meter-card').remove();
  }

  function preparePayload() {
    const accountsData = [];
    document.querySelectorAll('.account-block').forEach(accBlock => {
      const account = {
        owner_name: accBlock.querySelector('.acc-owner').value,
        address: accBlock.querySelector('.acc-address').value,
        account_number: accBlock.querySelector('.acc-number').value,
        valuation: accBlock.querySelector('.acc-val').value,
        rates_amount: accBlock.querySelector('.acc-rates').value,
        arrangement_amount: accBlock.querySelector('.acc-arr-amount').value,
        arrangement_duration: accBlock.querySelector('.acc-arr-dur').value,
        meters: []
      };
      
      accBlock.querySelectorAll('.meter-card').forEach(card => {
        account.meters.push({
          utility_type: card.dataset.type,
          meter_number: card.querySelector('.meter-number').value,
          assignment: card.querySelector('.meter-assignment').value,
          status: card.querySelector('.meter-status').value,
          prev_date: card.querySelector('.meter-prev-date').value,
          curr_date: card.querySelector('.meter-curr-date').value,
          prev_read: card.querySelector('.meter-prev-read').value,
          curr_read: card.querySelector('.meter-curr-read').value
        });
      });
      
      accountsData.push(account);
    });
    
    document.getElementById('payloadJson').value = JSON.stringify(accountsData);
  }
  
  // Auto-initialize first account on load
  window.addEventListener('DOMContentLoaded', () => {
    if (accountCount === 0) addAccount();
  });
</script>
{% endblock %}"""

with open('templates/program_billing/manual_capture.html', 'w', encoding='utf-8') as f:
    f.write(html_content)
print("manual_capture.html completely rewritten into Account Builder.")
