import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. FIX THE ACTION BLOCK
old_block_start = content.find('<!-- Status & Actions -->')
if old_block_start != -1:
    # Find the end of the Status & Actions block (which ends before <!-- Details Grid -->)
    old_block_end = content.find('<!-- Details Grid -->', old_block_start)
    if old_block_end != -1:
        new_action_block = '''<!-- Row 2: Actions & Status -->
      <div class="px-6 py-4 bg-slate-50 border-b border-slate-100 flex flex-wrap justify-between items-center gap-4 mb-8 rounded-xl shadow-sm">
        <div class="flex items-center gap-3">
          <span class="text-sm font-bold text-slate-500 uppercase tracking-wider">Status:</span>
          <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold 
            {% if job_card.status == 'Billed' %}bg-green-100 text-green-800
            {% elif job_card.status in ['Approved', 'Awaiting Deposit'] %}bg-blue-100 text-blue-800
            {% elif job_card.status == 'Rejected' %}bg-slate-100 text-slate-800
            {% else %}bg-amber-100 text-amber-800{% endif %}">
            {{ job_card.status }}
          </span>
        </div>
        
        <div class="flex items-center justify-end gap-2 flex-wrap">
          {% if job_card.status == 'Quote' %}
            <form method="POST" action="{{ url_for('mechanic_bp.accept_quote', id=job_card.id) }}" class="inline m-0">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                <button type="submit" class="px-4 py-2 bg-green-600 text-white font-bold rounded-lg hover:bg-green-700 shadow-sm transition text-sm">Accept Quote</button>
            </form>
            <button onclick="document.getElementById('reject-quote-modal').classList.remove('hidden')" class="px-4 py-2 border border-slate-300 bg-white text-slate-700 font-bold rounded-lg hover:bg-slate-50 shadow-sm transition text-sm">Reject Quote</button>
          {% elif job_card.status == 'Awaiting Deposit' %}
            <button onclick="document.getElementById('record-deposit-modal').classList.remove('hidden')" class="px-4 py-2 bg-indigo-600 text-white font-bold rounded-lg hover:bg-indigo-700 shadow-sm transition text-sm">Record Deposit</button>
          {% endif %}
          
          {% if client_debtor %}
            <a href="{{ url_for('debtors_bp.generate_soa', debtor_id=client_debtor.id) }}" class="px-4 py-2 border-2 border-indigo-600 text-indigo-700 bg-indigo-50 font-bold rounded-lg hover:bg-indigo-100 shadow-sm transition text-sm">View in Debtors</a>
          {% endif %}

          <div class="h-6 border-l border-slate-300 mx-1"></div>

          {% if not job_card.vehicle.client.email or not job_card.vehicle.client.phone or not job_card.vehicle.vin %}
            <button type="button" onclick="document.getElementById('missing-contact-modal').classList.remove('hidden'); document.getElementById('missing-contact-form').action='{{ url_for('mechanic_bp.update_client', client_id=job_card.vehicle.client.id, job_id=job_card.id, return_url=url_for('mechanic_bp.download_document', id=job_card.id)) }}';" class="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center text-slate-500 hover:bg-slate-200 hover:text-slate-800 transition" title="Download PDF (Missing Contact)">
              <i class="fas fa-file-pdf"></i>
            </button>
            <button type="button" onclick="document.getElementById('missing-contact-modal').classList.remove('hidden'); document.getElementById('missing-contact-form').action='{{ url_for('mechanic_bp.update_client', client_id=job_card.vehicle.client.id, job_id=job_card.id, return_url=url_for('mechanic_bp.email_document', id=job_card.id)) }}';" class="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center text-slate-500 hover:bg-slate-200 hover:text-slate-800 transition" title="Email (Missing Contact)">
              <i class="fas fa-paper-plane"></i>
            </button>
          {% else %}
            <a href="{{ url_for('mechanic_bp.download_document', id=job_card.id) }}" class="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center text-slate-600 hover:bg-slate-200 hover:text-slate-800 transition" title="Download PDF">
              <i class="fas fa-file-pdf"></i>
            </a>
            <a href="{{ url_for('mechanic_bp.email_document', id=job_card.id) }}" class="w-10 h-10 rounded-full bg-indigo-50 flex items-center justify-center text-indigo-600 hover:bg-indigo-100 hover:text-indigo-800 transition" title="Email {{ 'SOA' if job_card.status in ['Approved', 'Billed'] else 'Quote' }}">
              <i class="fas fa-paper-plane"></i>
            </a>
          {% endif %}
        </div>
      </div>
      
        <!-- Details Grid -->'''
        content = content[:old_block_start] + new_action_block + content[old_block_end + len('<!-- Details Grid -->'):]

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
