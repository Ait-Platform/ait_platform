import sys

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read().replace('\r\n', '\n')

# 1. Update the Phone Icon trigger
old_phone_btn = '''                  <a href="javascript:void(0)" onclick="document.getElementById('contact-modal-{{ job.id }}').classList.remove('hidden')" class="text-indigo-500 hover:text-indigo-700 font-semibold bg-indigo-50 px-2 py-1.5 rounded-md transition hover:bg-indigo-100 border border-indigo-200" title="Contact Client">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z"></path></svg>
                      </a>'''

new_phone_btn = '''                  {% if not job.vehicle.client.phone or not job.vehicle.client.email %}
                    <a href="javascript:void(0)" onclick="document.getElementById('missing-contact-modal-{{ job.id }}').classList.remove('hidden')" class="text-indigo-500 hover:text-indigo-700 font-semibold bg-indigo-50 px-2 py-1.5 rounded-md transition hover:bg-indigo-100 border border-indigo-200" title="Missing Contact Info">
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z"></path></svg>
                    </a>
                  {% else %}
                    <a href="javascript:void(0)" onclick="document.getElementById('contact-modal-{{ job.id }}').classList.remove('hidden')" class="text-indigo-500 hover:text-indigo-700 font-semibold bg-indigo-50 px-2 py-1.5 rounded-md transition hover:bg-indigo-100 border border-indigo-200" title="Contact Client">
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z"></path></svg>
                    </a>
                  {% endif %}'''

if old_phone_btn in content:
    content = content.replace(old_phone_btn, new_phone_btn)
else:
    print("old_phone_btn not found")


contact_modal_target = '''                    <!-- Contact Modal for this job -->'''

missing_modal = '''                    <!-- Missing Contact Modal for this job -->
                    <div id="missing-contact-modal-{{ job.id }}" class="fixed inset-0 z-50 hidden overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
                      <div class="flex items-end justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
                        <div class="fixed inset-0 transition-opacity bg-slate-500 bg-opacity-75" aria-hidden="true" onclick="document.getElementById('missing-contact-modal-{{ job.id }}').classList.add('hidden')"></div>
                        <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
                        <div class="inline-block overflow-hidden text-left align-bottom transition-all transform bg-white rounded-xl shadow-xl sm:my-8 sm:align-middle sm:max-w-md sm:w-full">
                          <div class="px-6 pt-6 pb-4 bg-white sm:p-6 sm:pb-4 border-b border-slate-100">
                            <h3 class="text-xl font-bold text-slate-900" id="modal-title">Missing Contact Details</h3>
                            <p class="mt-2 text-sm text-slate-500">Please provide the client's email and phone number before proceeding.</p>
                          </div>
                          <div class="p-6">
                            <form method="POST" action="{{ url_for('mechanic_bp.update_client', client_id=job.vehicle.client.id, job_id=job.id, return_url=url_for('mechanic_bp.job_cards_list')) }}">
                              <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                              <div class="space-y-4">
                                <div>
                                  <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
                                  <input type="text" name="phone" value="{{ job.vehicle.client.phone or '' }}" required class="block w-full rounded-lg border border-slate-300 p-2 text-sm">
                                </div>
                                <div>
                                  <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
                                  <input type="email" name="email" value="{{ job.vehicle.client.email or '' }}" required class="block w-full rounded-lg border border-slate-300 p-2 text-sm">
                                </div>
                              </div>
                              <div class="flex justify-end gap-3 mt-6">
                                <button type="button" onclick="document.getElementById('missing-contact-modal-{{ job.id }}').classList.add('hidden')" class="px-4 py-2 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-50">Cancel</button>
                                <button type="submit" class="px-4 py-2 bg-indigo-600 text-white font-bold rounded-lg hover:bg-indigo-700 shadow-sm">Save Details</button>
                              </div>
                            </form>
                          </div>
                        </div>
                      </div>
                    </div>

                    <!-- Contact Modal for this job -->'''

if contact_modal_target in content:
    content = content.replace(contact_modal_target, missing_modal)
else:
    print("contact_modal_target not found")


amber_box_target = '''                                {% if not job.vehicle.client.phone or not job.vehicle.client.email %}
                                <div class="mb-4 p-4 bg-amber-50 border border-amber-200 rounded-lg">
                                  <p class="text-sm text-amber-800 font-bold mb-3">Missing Contact Details. Please update:</p>
                                  <form method="POST" action="{{ url_for('mechanic_bp.update_client', client_id=job.vehicle.client.id, job_id=job.id) }}">
                                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                    <div class="grid grid-cols-1 gap-3 mb-3">
                                      {% if not job.vehicle.client.phone %}
                                      <input type="text" name="phone" placeholder="Phone Number" class="w-full rounded-md border-slate-300 p-2 text-sm" required>
                                      {% endif %}
                                      {% if not job.vehicle.client.email %}
                                      <input type="email" name="email" placeholder="Email Address" class="w-full rounded-md border-slate-300 p-2 text-sm" required>
                                      {% endif %}
                                    </div>
                                    <button type="submit" class="w-full bg-amber-600 hover:bg-amber-700 text-white font-bold py-2 px-4 rounded transition text-sm">Save Details</button>
                                  </form>
                                </div>
                                {% endif %}
                                
                                {% if job.vehicle.client.phone %}'''

amber_box_replacement = '''                                {% if job.vehicle.client.phone %}'''

if amber_box_target in content:
    content = content.replace(amber_box_target, amber_box_replacement)
else:
    print("amber_box_target not found")

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Script finished.")
