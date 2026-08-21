import sys

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update SOA button condition
content = content.replace(
    "{% if job_card.status != 'Quote' %}",
    "{% if job_card.status in ['Approved', 'In Progress', 'Quality Check', 'Ready', 'Billed'] %}"
)

# 2. Inject Modal right before Edit Client Modal
modal_html = '''
  <!-- Missing Contact for Email Modal -->
  <div id="missing-contact-modal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
    <div class="bg-white rounded-2xl w-full max-w-md shadow-2xl overflow-hidden animate-[fadeIn_0.2s_ease-out]">
      <div class="p-6">
        <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
          <h2 class="text-xl font-bold text-slate-800">Missing Contact Details</h2>
          <button type="button" onclick="document.getElementById('missing-contact-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 transition">
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
          </button>
        </div>
        <p class="text-slate-600 mb-6 text-sm">Please provide the client's email and phone number before sending documents.</p>
        
        <form method="POST" action="{{ url_for('mechanic_bp.update_client', client_id=job_card.vehicle.client.id, job_id=job_card.id, return_url=url_for('mechanic_bp.email_document', id=job_card.id)) }}">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
          
          <div class="space-y-4">
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
              <input type="text" name="phone" value="{{ job_card.vehicle.client.phone or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
              <input type="email" name="email" value="{{ job_card.vehicle.client.email or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
            </div>
          </div>
          
          <div class="flex justify-end gap-3 mt-8 border-t border-slate-100 pt-4">
            <button type="button" onclick="document.getElementById('missing-contact-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-50 transition">Cancel</button>
            <button type="submit" class="px-5 py-2.5 rounded-lg bg-indigo-600 text-white font-bold hover:bg-indigo-700 shadow-sm transition flex items-center">Save & Continue</button>
          </div>
        </form>
      </div>
    </div>
  </div>

  <!-- Edit Client Modal -->
'''
content = content.replace("  <!-- Edit Client Modal -->\n", modal_html)

# 3. Update the Email Quote button logic
email_btn_target = '''            <a href="{{ url_for('mechanic_bp.email_document', id=job_card.id) }}" class="inline-flex items-center rounded-lg border border-indigo-200 bg-white px-4 py-2 text-sm font-semibold text-indigo-700 shadow-sm hover:bg-indigo-50 transition">
              <i class="fas fa-paper-plane mr-2"></i> Email {{ 'SOA' if job_card.status in ['Approved', 'Billed'] else 'Quote' }}
            </a>'''

email_btn_replacement = '''            {% if not job_card.vehicle.client.email or not job_card.vehicle.client.phone %}
              <button type="button" onclick="document.getElementById('missing-contact-modal').classList.remove('hidden')" class="inline-flex items-center rounded-lg border border-indigo-200 bg-white px-4 py-2 text-sm font-semibold text-indigo-700 shadow-sm hover:bg-indigo-50 transition">
                <i class="fas fa-paper-plane mr-2"></i> Email {{ 'SOA' if job_card.status in ['Approved', 'Billed'] else 'Quote' }}
              </button>
            {% else %}
              <a href="{{ url_for('mechanic_bp.email_document', id=job_card.id) }}" class="inline-flex items-center rounded-lg border border-indigo-200 bg-white px-4 py-2 text-sm font-semibold text-indigo-700 shadow-sm hover:bg-indigo-50 transition">
                <i class="fas fa-paper-plane mr-2"></i> Email {{ 'SOA' if job_card.status in ['Approved', 'Billed'] else 'Quote' }}
              </a>
            {% endif %}'''

if email_btn_target in content:
    content = content.replace(email_btn_target, email_btn_replacement)
else:
    print("WARNING: Could not find email button target to replace!")

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Patch applied.")
