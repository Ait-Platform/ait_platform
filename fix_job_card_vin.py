import sys

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update all condition checks
old_cond = '''{% if not job_card.vehicle.client.email or not job_card.vehicle.client.phone %}'''
new_cond = '''{% if not job_card.vehicle.client.email or not job_card.vehicle.client.phone or not job_card.vehicle.vin %}'''
content = content.replace(old_cond, new_cond)

# 2. Add VIN to missing-contact-modal
old_modal_msg = '''<p class="text-slate-600 mb-6 text-sm">Please provide the client's email and phone number before proceeding.</p>'''
new_modal_msg = '''<p class="text-slate-600 mb-6 text-sm">Please provide the client's email, phone number, and vehicle VIN before proceeding.</p>'''
content = content.replace(old_modal_msg, new_modal_msg)

old_inputs = '''            <div class="space-y-4">
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
                <input type="text" name="phone" value="{{ job_card.vehicle.client.phone or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
                <input type="email" name="email" value="{{ job_card.vehicle.client.email or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
            </div>'''
new_inputs = '''            <div class="space-y-4">
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
                <input type="text" name="phone" value="{{ job_card.vehicle.client.phone or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
                <input type="email" name="email" value="{{ job_card.vehicle.client.email or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Vehicle VIN</label>
                <input type="text" name="vin" value="{{ job_card.vehicle.vin or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
            </div>'''
content = content.replace(old_inputs, new_inputs)

# 3. Add to edit-client-modal too, just in case they use that!
old_edit_inputs = '''              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
                <input type="email" name="email" value="{{ job_card.vehicle.client.email or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
            </div>'''
new_edit_inputs = '''              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
                <input type="email" name="email" value="{{ job_card.vehicle.client.email or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Vehicle VIN</label>
                <input type="text" name="vin" value="{{ job_card.vehicle.vin or '' }}" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
            </div>'''
content = content.replace(old_edit_inputs, new_edit_inputs)


with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
