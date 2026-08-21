import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1.1 Remove POP button
pop_btn_original = '''            {% if job_card.status == 'Quote' %}
              <button type="button" onclick="document.getElementById('approve-quote-modal').classList.remove('hidden')" class="inline-flex items-center rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-green-700 transition">
                Capture POP & Approve
              </button>
            {% else %}'''

pop_btn_new = '''            {% if job_card.status != 'Quote' %}'''
content = content.replace(pop_btn_original, pop_btn_new)

# 1.1 Remove POP modal entirely
modal_regex = r'<!-- Approve Quote Modal \(Capture POP\) -->.*?</div>\s*</div>\s*</div>'
content = re.sub(modal_regex, '', content, flags=re.DOTALL)

# 2.1 Add Edit button to Client Details and modal
client_details_original = '''        <div class="bg-white border border-slate-200 rounded-xl p-5 shadow-sm">
          <h3 class="text-sm font-bold text-slate-500 uppercase tracking-wider mb-4 border-b border-slate-100 pb-2">Client Details</h3>
          <p class="font-bold text-slate-900 text-lg mb-1">{{ job_card.vehicle.client.name }}</p>
          <p class="text-slate-600 text-sm"><i class="fas fa-phone mr-2 text-slate-400"></i>{{ job_card.vehicle.client.phone or 'No phone' }}</p>
          <p class="text-slate-600 text-sm mt-1"><i class="fas fa-envelope mr-2 text-slate-400"></i>{{ job_card.vehicle.client.email or 'No email' }}</p>
        </div>'''

client_details_new = '''        <div class="bg-white border border-slate-200 rounded-xl p-5 shadow-sm relative group">
          <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
            <h3 class="text-sm font-bold text-slate-500 uppercase tracking-wider">Client Details</h3>
            <button type="button" onclick="document.getElementById('edit-client-modal').classList.remove('hidden')" class="text-xs font-semibold text-indigo-600 hover:text-indigo-800 transition px-2 py-1 bg-indigo-50 rounded hidden group-hover:block border border-indigo-200">
              <i class="fas fa-edit mr-1"></i>Edit
            </button>
          </div>
          <p class="font-bold text-slate-900 text-lg mb-1">{{ job_card.vehicle.client.name }}</p>
          <p class="text-slate-600 text-sm"><i class="fas fa-phone mr-2 text-slate-400"></i>{{ job_card.vehicle.client.phone or 'No phone' }}</p>
          <p class="text-slate-600 text-sm mt-1"><i class="fas fa-envelope mr-2 text-slate-400"></i>{{ job_card.vehicle.client.email or 'No email' }}</p>
        </div>'''
content = content.replace(client_details_original, client_details_new)

# Append Edit Client Modal carefully at the end of the content block
client_modal = '''
  <!-- Edit Client Modal -->
  <div id="edit-client-modal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
    <div class="bg-white rounded-2xl w-full max-w-md shadow-2xl overflow-hidden animate-[fadeIn_0.2s_ease-out]">
      <div class="p-6">
        <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
          <h2 class="text-xl font-bold text-slate-800">Edit Client Details</h2>
          <button type="button" onclick="document.getElementById('edit-client-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 transition">
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
          </button>
        </div>
        <p class="text-slate-600 mb-6 text-sm">Updates here will be saved to both the active Quote and the client's Debtors (SOA) profile.</p>
        
        <form method="POST" action="{{ url_for('mechanic_bp.update_client', client_id=job_card.vehicle.client.id, job_id=job_card.id) }}">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
          
          <div class="space-y-4">
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Name</label>
              <input type="text" name="name" value="{{ job_card.vehicle.client.name }}" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
            </div>
            
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
              <input type="text" name="phone" value="{{ job_card.vehicle.client.phone or '' }}" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
            </div>
            
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
              <input type="email" name="email" value="{{ job_card.vehicle.client.email or '' }}" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
            </div>
          </div>
          
          <div class="flex justify-end gap-3 mt-8 border-t border-slate-100 pt-4">
            <button type="button" onclick="document.getElementById('edit-client-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-50 transition">Cancel</button>
            <button type="submit" class="px-5 py-2.5 rounded-lg bg-indigo-600 text-white font-bold hover:bg-indigo-700 shadow-sm transition flex items-center">Save Changes</button>
          </div>
        </form>
      </div>
    </div>
  </div>
{% endblock %}
'''

# Find the LAST {% endblock %} and replace it
last_endblock_index = content.rfind('{% endblock %}')
if last_endblock_index != -1:
    content = content[:last_endblock_index] + client_modal + content[last_endblock_index + 14:]

# 3.1 Separate Year from Model in Vehicle Details
vehicle_details_original = '''          <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Model:</span> {{ job_card.vehicle.model or 'Unknown' }} ({{ job_card.vehicle.year or 'N/A' }})</p>'''
vehicle_details_new = '''          <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Model:</span> {{ job_card.vehicle.model or 'Unknown' }}</p>
          <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Year:</span> {{ job_card.vehicle.year or 'N/A' }}</p>'''
content = content.replace(vehicle_details_original, vehicle_details_new)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated job_card.html safely")
