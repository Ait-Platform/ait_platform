import sys

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update all condition checks
old_cond = '''{% if not job.vehicle.client.email or not job.vehicle.client.phone %}'''
new_cond = '''{% if not job.vehicle.client.email or not job.vehicle.client.phone or not job.vehicle.vin %}'''
content = content.replace(old_cond, new_cond)

# 2. Add VIN to missing-contact-modal
old_modal_msg = '''<p class="text-slate-600 mb-6 text-sm">Please provide the client's email and phone number before proceeding.</p>'''
new_modal_msg = '''<p class="text-slate-600 mb-6 text-sm">Please provide the client's email, phone number, and vehicle VIN before proceeding.</p>'''
content = content.replace(old_modal_msg, new_modal_msg)

# For job_cards_list, the inputs are inside a loop so they use job variable
old_inputs = '''            <div class="space-y-4">
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
                <input type="text" name="phone" value="{{ job.vehicle.client.phone or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
                <input type="email" name="email" value="{{ job.vehicle.client.email or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
            </div>'''
new_inputs = '''            <div class="space-y-4">
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
                <input type="text" name="phone" value="{{ job.vehicle.client.phone or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
                <input type="email" name="email" value="{{ job.vehicle.client.email or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Vehicle VIN</label>
                <input type="text" name="vin" value="{{ job.vehicle.vin or '' }}" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2">
              </div>
            </div>'''
content = content.replace(old_inputs, new_inputs)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
