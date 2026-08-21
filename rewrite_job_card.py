import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update the Actions Block (Lines 45-68)
actions_original = '''          <div class="flex items-center gap-3">
            {% if job_card.status == 'Billed' %}
              <a href="{{ url_for('mechanic_bp.client_soa', client_id=job_card.vehicle.client.id) }}" class="inline-flex items-center rounded-lg border border-indigo-200 bg-white px-4 py-2 text-sm font-semibold text-indigo-700 shadow-sm hover:bg-slate-50 transition">
                View Client SOA
              </a>
              <a href="{{ url_for('mechanic_bp.generate_invoice', id=job_card.id) }}" class="inline-flex items-center rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-indigo-700 transition">
                View Invoice
              </a>
            {% elif job_card.status == 'Quote' %}
              <button type="button" onclick="document.getElementById('approve-quote-modal').classList.remove('hidden')" class="inline-flex items-center rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-green-700 transition">
                Approve Quote & Start Job
              </button>
            {% else %}
              <form action="{{ url_for('mechanic_bp.generate_invoice', id=job_card.id) }}" method="POST" class="inline">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <button type="submit" class="inline-flex items-center rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-indigo-700 transition">
                  Generate Final Invoice
                </button>
              </form>
            {% endif %}
            <a href="{{ url_for('mechanic_bp.email_document', id=job_card.id) }}" class="inline-flex items-center rounded-lg border border-indigo-200 bg-white px-4 py-2 text-sm font-semibold text-indigo-700 shadow-sm hover:bg-indigo-50 transition">
              <i class="fas fa-paper-plane mr-2"></i> Email {{ 'Invoice' if job_card.status == 'Billed' else 'Quote' }}
            </a>
          </div>'''

actions_new = '''          <div class="flex items-center gap-3">
            {% if job_card.status == 'Quote' %}
              <button type="button" onclick="document.getElementById('approve-quote-modal').classList.remove('hidden')" class="inline-flex items-center rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-green-700 transition">
                Capture POP & Approve
              </button>
            {% else %}
              <a href="{{ url_for('mechanic_bp.client_soa', client_id=job_card.vehicle.client.id, return_url=url_for('mechanic_bp.job_card_detail', id=job_card.id)) }}" class="inline-flex items-center rounded-lg border border-indigo-200 bg-white px-4 py-2 text-sm font-semibold text-indigo-700 shadow-sm hover:bg-slate-50 transition">
                View Client SOA
              </a>
            {% endif %}
            <a href="{{ url_for('mechanic_bp.email_document', id=job_card.id) }}" class="inline-flex items-center rounded-lg border border-indigo-200 bg-white px-4 py-2 text-sm font-semibold text-indigo-700 shadow-sm hover:bg-indigo-50 transition">
              <i class="fas fa-paper-plane mr-2"></i> Email {{ 'SOA' if job_card.status in ['Approved', 'Billed'] else 'Quote' }}
            </a>
          </div>'''

content = content.replace(actions_original, actions_new)

# 2. Update Document Title Logic
title_original = '''            {% if job_card.status == 'Quote' %}Quotation
            {% elif job_card.status == 'Billed' %}Tax Invoice
            {% else %}Job Card
            {% endif %} #{{ job_card.job_number }}'''

title_new = '''            {% if job_card.status == 'Quote' %}Quote / Tax Invoice
            {% elif job_card.status in ['Approved', 'Billed'] %}Tax Invoice
            {% else %}Job Card
            {% endif %} #{{ job_card.job_number }}'''

content = content.replace(title_original, title_new)

# 3. Update the approve-quote-modal
modal_original = '''  <!-- Approve Quote Modal -->
  <div id="approve-quote-modal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
    <div class="bg-white rounded-2xl w-full max-w-md shadow-2xl overflow-hidden animate-[fadeIn_0.2s_ease-out]">
      <div class="p-6">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-xl font-bold text-slate-800">Approve Quote & Start Job</h2>
          <button type="button" onclick="document.getElementById('approve-quote-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 transition">
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
          </button>
        </div>
        <p class="text-slate-600 mb-6 text-sm">Approving this quote will change its status to 'Approved' and allow you to capture an optional deposit. The deposit will automatically create a Debtors (SOA) entry for this client.</p>
        
        <form method="POST" action="{{ url_for('mechanic_bp.approve_quote', id=job_card.id) }}">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
          
          <div class="mb-6">
            <label class="block text-sm font-bold text-slate-700 mb-2">Deposit Amount Received ({{ currency_sym }}) <span class="text-slate-400 font-normal">(Optional)</span></label>
            <input type="number" step="0.01" min="0" name="deposit_amount" class="block w-full rounded-lg border-2 border-slate-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-lg p-3 transition font-medium text-slate-800" placeholder="0.00">
          </div>
          
          <div class="flex justify-end gap-3 mt-8">
            <button type="button" onclick="document.getElementById('approve-quote-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-50 transition">Cancel</button>
            <button type="submit" class="px-5 py-2.5 rounded-lg bg-green-600 text-white font-bold hover:bg-green-700 shadow-sm transition">Approve & Start</button>
          </div>
        </form>
      </div>
    </div>
  </div>'''

modal_new = '''  <!-- Approve Quote Modal (Capture POP) -->
  <div id="approve-quote-modal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
    <div class="bg-white rounded-2xl w-full max-w-md shadow-2xl overflow-hidden animate-[fadeIn_0.2s_ease-out]">
      <div class="p-6">
        <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
          <h2 class="text-xl font-bold text-slate-800">Capture Proof of Payment</h2>
          <button type="button" onclick="document.getElementById('approve-quote-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 transition">
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
          </button>
        </div>
        <p class="text-slate-600 mb-6 text-sm">Capturing a deposit triggers the legal shift from a Quote to a Tax Invoice. The initial total will be charged to the client's Debtors account, and this deposit will be credited against it.</p>
        
        <form method="POST" action="{{ url_for('mechanic_bp.approve_quote', id=job_card.id) }}">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
          
          <div class="space-y-4">
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">POP Date</label>
              <input type="date" name="pop_date" value="{{ today_date }}" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
            </div>
            
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">POP Reference / Note</label>
              <input type="text" name="pop_ref" value="POP-{{ job_card.job_number }}" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
            </div>
            
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Deposit Amount Received ({{ currency_sym }}) <span class="text-slate-400 font-normal">(Optional)</span></label>
              <input type="number" step="0.01" min="0" name="pop_amount" class="block w-full rounded-lg border-2 border-green-300 focus:border-green-500 focus:ring-green-500 text-lg p-3 font-medium text-slate-900 bg-green-50" placeholder="0.00">
            </div>
          </div>
          
          <div class="flex justify-end gap-3 mt-8 border-t border-slate-100 pt-4">
            <button type="button" onclick="document.getElementById('approve-quote-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-50 transition">Cancel</button>
            <button type="submit" class="px-5 py-2.5 rounded-lg bg-green-600 text-white font-bold hover:bg-green-700 shadow-sm transition flex items-center"><i class="fas fa-check-circle mr-2"></i> Save & Approve</button>
          </div>
        </form>
      </div>
    </div>
  </div>'''

content = content.replace(modal_original, modal_new)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated job_card.html successfully")
