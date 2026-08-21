import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

missing_contact_start = content.find('<!-- Missing Contact for Email Modal -->')
edit_client_start = content.find('<!-- Edit Client Modal -->')

new_missing_contact = '''<!-- Missing Contact for Email Modal -->
    <div id="missing-contact-modal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div class="bg-white rounded-2xl w-full max-w-md shadow-2xl overflow-y-auto max-h-[90vh] animate-[fadeIn_0.2s_ease-out]">
        <div class="p-6">
          <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
            <h2 class="text-xl font-bold text-slate-800">Missing Contact Details</h2>
            <button type="button" onclick="document.getElementById('missing-contact-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 transition">
              <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
            </button>
          </div>
          <p class="text-slate-600 mb-6 text-sm">Please provide the client's email, phone number, and vehicle VIN before proceeding.</p>
          
          <form id="missing-contact-form" method="POST" action="{{ url_for('mechanic_bp.update_client', client_id=job_card.vehicle.client.id, job_id=job_card.id, return_url=url_for('mechanic_bp.email_document', id=job_card.id)) }}">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
            
            <div class="space-y-4">
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Phone <span class="text-xs text-red-500">*</span></label>
                <input type="text" name="phone" id="missing_phone" value="{{ job_card.vehicle.client.phone or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Email <span class="text-xs text-red-500">*</span></label>
                <input type="email" name="email" id="missing_email" value="{{ job_card.vehicle.client.email or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
              
              <div class="pt-2 border-t border-slate-100">
                <label class="block text-sm font-bold text-slate-700 mb-1">Upload License Disk (Optional AI Extract)</label>
                <input type="file" id="ajax_disk_upload" accept="image/*" class="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-indigo-50 file:text-indigo-700 hover:file:bg-indigo-100 cursor-pointer border border-slate-300 rounded-md shadow-sm">
                <p id="upload_status" class="text-xs text-indigo-600 font-semibold mt-1 hidden">Uploading...</p>
              </div>

              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Vehicle VIN <span class="text-xs text-red-500">*</span></label>
                <input type="text" name="vin" id="missing_vin" value="{{ job_card.vehicle.vin or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2 uppercase">
              </div>
            </div>
            
            <div class="flex justify-end gap-3 mt-8 border-t border-slate-100 pt-4">
              <button type="button" onclick="document.getElementById('missing-contact-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-50 transition">Cancel</button>
              <button type="submit" onclick="if(document.getElementById('missing_phone').value && document.getElementById('missing_email').value && document.getElementById('missing_vin').value) { document.getElementById('missing-contact-modal').classList.add('hidden'); }" class="px-5 py-2.5 rounded-lg bg-indigo-600 text-white font-bold hover:bg-indigo-700 shadow-sm transition flex items-center">Save & Continue</button>
            </div>
          </form>
        </div>
      </div>
    </div>
    
'''

content = content[:missing_contact_start] + new_missing_contact + content[edit_client_start:]

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
