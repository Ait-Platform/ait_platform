html_content = """{% extends "layout.html" %}

{% block title %}Phase 2: Initial Data Capture{% endblock %}

{% block content %}
<div class="max-w-5xl mx-auto px-4 py-8">
  <form method="POST" action="{{ url_for('billing_bp.save_readings') }}" id="readingsForm">
    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
    <input type="hidden" name="property_id" value="{{ property.id }}">
    
    <!-- JSON payload to send accounts and readings dynamically -->
    <input type="hidden" name="payload_json" id="payloadJson">

    <div class="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
      <!-- Color strip on top -->
      <div class="h-2 bg-emerald-500"></div>

      <div class="p-6 md:p-8 space-y-6">
        <!-- Header -->
        <div class="flex items-center justify-between border-b border-slate-100 pb-4">
          <div>
            <h1 class="text-2xl md:text-3xl font-extrabold text-slate-900 tracking-tight">Phase 2: Initial Data Capture</h1>
            <p class="text-sm text-slate-500 mt-1">Property: <span class="font-semibold text-slate-700">{{ property.name }}</span></p>
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
              <h3 class="text-sm font-bold text-blue-900 uppercase tracking-wide">Data Entry</h3>
              <p class="text-sm text-blue-800 mt-1">Your architecture is mapped. Now, enter the initial meter readings and financial rates for each of your {{ accounts|length }} accounts to establish the baseline.</p>
            </div>
          </div>
        </div>

        <!-- Accounts Accordion -->
        <div class="space-y-4">
          {% for acc in accounts %}
          <div class="account-block bg-white rounded-xl shadow-sm border-2 border-slate-200 overflow-hidden" data-acc-id="{{ acc.id }}">
            <div class="bg-slate-50 px-6 py-4 border-b border-slate-200 flex justify-between items-center cursor-pointer hover:bg-slate-100 transition" onclick="toggleAccordion('acc_{{ acc.id }}')">
              <div>
                <h3 class="font-bold text-lg text-slate-800 flex items-center">
                  Account: <span class="text-blue-700 ml-2 acc-number">{{ acc.account_number or 'Unnumbered' }}</span>
                </h3>
                <p class="text-xs text-slate-500 mt-1">Owner: {{ acc.owner.name if acc.owner else 'None' }}</p>
              </div>
              <svg id="icon_acc_{{ acc.id }}" class="w-6 h-6 text-slate-400 transform transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
            </div>
            
            <div id="content_acc_{{ acc.id }}" class="p-6 hidden space-y-6">
              
              <!-- Financials -->
              <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div class="bg-white p-4 rounded-lg border border-slate-200 shadow-sm">
                  <h4 class="font-bold text-slate-800 mb-3 border-b pb-2">Property Rates & Valuation</h4>
                  <div class="grid grid-cols-2 gap-4">
                    <div>
                      <label class="block text-xs font-semibold text-slate-600 mb-1">Valuation (R)</label>
                      <input type="number" step="0.01" class="acc-val w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500" value="{{ acc.valuation or '' }}">
                    </div>
                    <div>
                      <label class="block text-xs font-semibold text-slate-600 mb-1">Monthly Rates (R)</label>
                      <input type="number" step="0.01" class="acc-rates w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500" value="{{ acc.rates_amount or '' }}">
                    </div>
                  </div>
                </div>

                <div class="bg-amber-50 p-4 rounded-lg border border-amber-200 shadow-sm">
                  <h4 class="font-bold text-amber-900 mb-3 border-b border-amber-200 pb-2">Arrears Arrangement</h4>
                  <div class="grid grid-cols-2 gap-4">
                    <div>
                      <label class="block text-xs font-semibold text-amber-800 mb-1">Monthly Payment (R)</label>
                      <input type="number" step="0.01" class="acc-arr-amt w-full rounded border border-amber-300 px-3 py-2 text-sm outline-none focus:border-amber-500 bg-white" value="{{ acc.arrangement_amount or '' }}">
                    </div>
                    <div>
                      <label class="block text-xs font-semibold text-amber-800 mb-1">Term (Months)</label>
                      <input type="number" class="acc-arr-dur w-full rounded border border-amber-300 px-3 py-2 text-sm outline-none focus:border-amber-500 bg-white" value="{{ acc.arrangement_duration or '' }}">
                    </div>
                  </div>
                </div>
              </div>

              <!-- Meters -->
              <div>
                <h4 class="text-sm font-bold text-slate-800 uppercase tracking-wide border-b pb-1 mb-4">Mapped Meters</h4>
                <div class="space-y-4">
                  {% set meters = get_meters_for_account(acc.account_number) %}
                  {% if not meters %}
                    <p class="text-sm text-slate-500 italic">No meters mapped to this account.</p>
                  {% endif %}
                  {% for meter in meters %}
                  <div class="meter-card bg-slate-50 p-4 rounded border border-slate-300 shadow-sm" data-meter-id="{{ meter.id }}" data-meter-no="{{ meter.meter_number }}">
                    <div class="flex items-center mb-3 justify-between">
                      <div class="flex items-center">
                        {% if meter.utility_type == 'water' %}
                          <svg class="w-5 h-5 text-sky-600 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"></path></svg>
                        {% else %}
                          <svg class="w-5 h-5 text-indigo-600 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
                        {% endif %}
                        <h5 class="font-bold text-md text-slate-800">{{ meter.utility_type|title }} Meter: <span class="text-blue-700">{{ meter.meter_number }}</span></h5>
                      </div>
                      <span class="px-2 py-1 bg-slate-200 text-slate-700 text-[10px] uppercase font-bold rounded">Status: {{ meter.status }}</span>
                    </div>
                    
                    <div class="grid grid-cols-2 md:grid-cols-4 gap-3 bg-white p-3 rounded-lg border border-slate-200">
                      <div>
                        <label class="block text-[10px] font-bold text-slate-600 mb-0.5">Prev Read Date</label>
                        <input type="date" class="m-pdate w-full rounded border border-slate-300 px-2 py-1.5 text-xs outline-none focus:border-emerald-500" required>
                      </div>
                      <div>
                        <label class="block text-[10px] font-bold text-slate-600 mb-0.5">Prev Reading</label>
                        <input type="number" step="0.01" class="m-pread w-full rounded border border-slate-300 px-2 py-1.5 text-xs outline-none focus:border-emerald-500" required>
                      </div>
                      <div>
                        <label class="block text-[10px] font-bold text-slate-600 mb-0.5">Curr Read Date</label>
                        <input type="date" class="m-cdate w-full rounded border border-slate-300 px-2 py-1.5 text-xs outline-none focus:border-emerald-500" required>
                      </div>
                      <div>
                        <label class="block text-[10px] font-bold text-slate-600 mb-0.5">Curr Reading</label>
                        <input type="number" step="0.01" class="m-cread w-full rounded border border-slate-300 px-2 py-1.5 text-xs outline-none focus:border-emerald-500" required>
                      </div>
                    </div>
                  </div>
                  {% endfor %}
                </div>
              </div>

            </div>
          </div>
          {% endfor %}
        </div>

        <div class="pt-8 border-t border-slate-100 flex justify-end">
          <button type="submit" onclick="preparePayload()" class="bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-3 px-8 rounded-xl shadow-md transition transform hover:-translate-y-0.5">
            Save Readings &amp; Finalize Baseline
          </button>
        </div>

      </div>
    </div>
  </form>
</div>

<script>
  function toggleAccordion(accId) {
    const content = document.getElementById('content_' + accId);
    const icon = document.getElementById('icon_' + accId);
    if (content.classList.contains('hidden')) {
      content.classList.remove('hidden');
      icon.classList.add('rotate-180');
    } else {
      content.classList.add('hidden');
      icon.classList.remove('rotate-180');
    }
  }

  function preparePayload() {
    const payload = [];
    document.querySelectorAll('.account-block').forEach(accBlock => {
      const accData = {
        id: accBlock.dataset.accId,
        account_number: accBlock.querySelector('.acc-number').textContent.trim(),
        valuation: accBlock.querySelector('.acc-val').value,
        rates_amount: accBlock.querySelector('.acc-rates').value,
        arr_amount: accBlock.querySelector('.acc-arr-amt').value,
        arr_dur: accBlock.querySelector('.acc-arr-dur').value,
        meters: []
      };

      accBlock.querySelectorAll('.meter-card').forEach(mCard => {
        accData.meters.push({
          meter_id: mCard.dataset.meterId,
          meter_number: mCard.dataset.meterNo,
          prev_date: mCard.querySelector('.m-pdate').value,
          prev_read: mCard.querySelector('.m-pread').value,
          curr_date: mCard.querySelector('.m-cdate').value,
          curr_read: mCard.querySelector('.m-cread').value
        });
      });

      payload.push(accData);
    });

    document.getElementById('payloadJson').value = JSON.stringify(payload);
  }
</script>
{% endblock %}"""

with open('templates/program_billing/capture_readings.html', 'w', encoding='utf-8') as f:
    f.write(html_content)
print("Created capture_readings.html")
