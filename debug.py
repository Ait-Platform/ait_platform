import sys

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_phone_btn = '''<a href="javascript:void(0)" onclick="document.getElementById('contact-modal-{{ job.id }}').classList.remove('hidden')" class="text-indigo-500 hover:text-indigo-700 font-semibold bg-indigo-50 px-2 py-1.5 rounded-md transition hover:bg-indigo-100 border border-indigo-200" title="Contact Client">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z"></path></svg>
                      </a>'''
print('old_phone_btn found:', old_phone_btn in content)

contact_modal_target = '''                    <!-- Contact Modal for this job -->'''
print('contact_modal_target found:', contact_modal_target in content)

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
print('amber_box_target found:', amber_box_target in content)
