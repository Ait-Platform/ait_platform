import re

# 1. Update job_card.html
with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    jc_content = f.read()

# Replace the onclick of the phone number
phone_regex = re.compile(r'<a href="javascript:void\(0\)" onclick=".*?\{\% if job_card\.vehicle\.client\.phone \%\}fetch.*?<i class="fas fa-phone mr-2 text-slate-400"></i>', re.DOTALL)
phone_replace = '''<a href="javascript:void(0)" onclick="{% if job_card.vehicle.client.phone %}document.getElementById('contact-modal').classList.remove('hidden'){% else %}alert('No phone number saved for this client. Please edit the client profile.');{% endif %}" class="hover:text-indigo-600 transition" title="Contact Client">
                <i class="fas fa-phone mr-2 text-slate-400"></i>'''
jc_content = phone_regex.sub(phone_replace, jc_content)

# Add the contact-modal at the bottom
modal_html = '''
    <!-- Contact Client Modal -->
    <div id="contact-modal" class="fixed inset-0 z-50 hidden overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
      <div class="flex items-end justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
        <div class="fixed inset-0 transition-opacity bg-slate-500 bg-opacity-75" aria-hidden="true" onclick="document.getElementById('contact-modal').classList.add('hidden')"></div>
        <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
        <div class="inline-block overflow-hidden text-left align-bottom transition-all transform bg-white rounded-xl shadow-xl sm:my-8 sm:align-middle sm:max-w-md sm:w-full">
          <div class="px-6 pt-6 pb-4 bg-white sm:p-6 sm:pb-4 border-b border-slate-100">
            <h3 class="text-xl font-bold text-slate-900" id="modal-title">Contact Client</h3>
            <p class="mt-2 text-sm text-slate-500">How would you like to contact {{ job_card.vehicle.client.name }} ({{ job_card.vehicle.client.phone }})?</p>
          </div>
          <div class="p-6">
            <div class="grid grid-cols-1 gap-4">
              <form method="POST" action="{{ url_for('mechanic_bp.log_contact', job_id=job_card.id) }}" target="_blank" onsubmit="setTimeout(() => { document.getElementById('contact-modal').classList.add('hidden'); window.location.reload(); }, 1000);">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                <input type="hidden" name="contact_type" value="WhatsApp">
                <button type="submit" onclick="window.location.href='https://wa.me/{{ job_card.vehicle.client.phone|replace(' ', '')|replace('+', '')|replace('-', '') }}'" class="w-full flex items-center justify-center gap-3 px-4 py-3 bg-[#25D366] text-white rounded-lg font-bold hover:bg-[#128C7E] transition shadow-sm">
                  <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51a12.8 12.8 0 0 0-.57-.01c-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 0 1-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 0 1-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 0 1 2.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0 0 12.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 0 0 5.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 0 0-3.48-8.413z"/></svg>
                  Message on WhatsApp
                </button>
              </form>
              
              <form method="POST" action="{{ url_for('mechanic_bp.log_contact', job_id=job_card.id) }}" target="_blank" onsubmit="setTimeout(() => { document.getElementById('contact-modal').classList.add('hidden'); window.location.reload(); }, 1000);">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                <input type="hidden" name="contact_type" value="Phone Call">
                <button type="submit" onclick="window.location.href='tel:{{ job_card.vehicle.client.phone }}'" class="w-full flex items-center justify-center gap-3 px-4 py-3 bg-indigo-600 text-white rounded-lg font-bold hover:bg-indigo-700 transition shadow-sm">
                  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z"></path></svg>
                  Call via Phone
                </button>
              </form>
            </div>
          </div>
          <div class="px-6 py-4 bg-slate-50 flex justify-end rounded-b-xl border-t border-slate-100">
            <button type="button" onclick="document.getElementById('contact-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-100 transition">Cancel</button>
          </div>
        </div>
      </div>
    </div>
'''

jc_content = jc_content.replace('{% endblock %}', modal_html + '\n{% endblock %}')

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(jc_content)
print("Updated job_card.html")


# 2. Update job_cards_list.html
with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    jcl_content = f.read()

# Replace the phone icon in the table
phone_icon_regex = re.compile(r'<a href="javascript:void\(0\)" onclick="\{\% if job\.vehicle\.client\.phone \%\}fetch.*?<i class="fas fa-phone"></i>\s*</a>', re.DOTALL)

phone_icon_replace = '''<a href="javascript:void(0)" onclick="{% if job.vehicle.client.phone %}document.getElementById('contact-modal-{{ job.id }}').classList.remove('hidden'){% else %}alert('No phone number saved for this client. Please edit the client profile inside the Job Card.');{% endif %}" class="text-indigo-500 hover:text-indigo-700 font-semibold bg-indigo-50 px-2 py-1.5 rounded-md transition hover:bg-indigo-100 border border-indigo-200" title="Contact Client">
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z"></path></svg>
                    </a>'''
jcl_content = phone_icon_regex.sub(phone_icon_replace, jcl_content)

# Inject the modals into the for loop just above the existing POP Modal
pop_modal_find = "<!-- POP Modal for this job -->"
contact_modal_list = '''<!-- Contact Modal for this job -->
                    <div id="contact-modal-{{ job.id }}" class="fixed inset-0 z-50 hidden overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
                      <div class="flex items-end justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
                        <div class="fixed inset-0 transition-opacity bg-slate-500 bg-opacity-75" aria-hidden="true" onclick="document.getElementById('contact-modal-{{ job.id }}').classList.add('hidden')"></div>
                        <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
                        <div class="inline-block overflow-hidden text-left align-bottom transition-all transform bg-white rounded-xl shadow-xl sm:my-8 sm:align-middle sm:max-w-md sm:w-full">
                          <div class="px-6 pt-6 pb-4 bg-white sm:p-6 sm:pb-4 border-b border-slate-100">
                            <h3 class="text-xl font-bold text-slate-900" id="modal-title">Contact Client</h3>
                            <p class="mt-2 text-sm text-slate-500">How would you like to contact {{ job.vehicle.client.name }} ({{ job.vehicle.client.phone }})?</p>
                          </div>
                          <div class="p-6">
                            <div class="grid grid-cols-1 gap-4">
                              <form method="POST" action="{{ url_for('mechanic_bp.log_contact', job_id=job.id) }}" target="_blank" onsubmit="setTimeout(() => { document.getElementById('contact-modal-{{ job.id }}').classList.add('hidden'); window.location.reload(); }, 1000);">
                                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                <input type="hidden" name="contact_type" value="WhatsApp">
                                <button type="submit" onclick="window.location.href='https://wa.me/{{ job.vehicle.client.phone|replace(' ', '')|replace('+', '')|replace('-', '') }}'" class="w-full flex items-center justify-center gap-3 px-4 py-3 bg-[#25D366] text-white rounded-lg font-bold hover:bg-[#128C7E] transition shadow-sm">
                                  <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51a12.8 12.8 0 0 0-.57-.01c-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 0 1-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 0 1-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 0 1 2.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0 0 12.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 0 0 5.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 0 0-3.48-8.413z"/></svg>
                                  Message on WhatsApp
                                </button>
                              </form>
                              
                              <form method="POST" action="{{ url_for('mechanic_bp.log_contact', job_id=job.id) }}" target="_blank" onsubmit="setTimeout(() => { document.getElementById('contact-modal-{{ job.id }}').classList.add('hidden'); window.location.reload(); }, 1000);">
                                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                <input type="hidden" name="contact_type" value="Phone Call">
                                <button type="submit" onclick="window.location.href='tel:{{ job.vehicle.client.phone }}'" class="w-full flex items-center justify-center gap-3 px-4 py-3 bg-indigo-600 text-white rounded-lg font-bold hover:bg-indigo-700 transition shadow-sm">
                                  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z"></path></svg>
                                  Call via Phone
                                </button>
                              </form>
                            </div>
                          </div>
                          <div class="px-6 py-4 bg-slate-50 flex justify-end rounded-b-xl border-t border-slate-100">
                            <button type="button" onclick="document.getElementById('contact-modal-{{ job.id }}').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-100 transition">Cancel</button>
                          </div>
                        </div>
                      </div>
                    </div>
                    
                    <!-- POP Modal for this job -->'''
jcl_content = jcl_content.replace(pop_modal_find, contact_modal_list)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(jcl_content)
print("Updated job_cards_list.html")
