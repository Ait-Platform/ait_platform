import re

with open('templates/program_billing/manual_capture.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix Help Modal placement (remove from top, put at bottom)
help_modal_regex = r'<!-- Help Modal -->.*?</div>\s*</div>'
match = re.search(help_modal_regex, content, flags=re.DOTALL)
if match:
    modal_str = match.group(0)
    content = content.replace(modal_str, '') # Remove from top
    content = content.replace('{% endblock %}', modal_str + '\n{% endblock %}') # Add to bottom

# Add Is Bulk Account checkbox to Tab 1
bulk_checkbox = """
        <div class="mb-4 bg-blue-50 border border-blue-200 p-4 rounded-lg flex items-center justify-between">
          <div>
            <h4 class="font-bold text-blue-900 text-sm">Designate as Bulk Account?</h4>
            <p class="text-xs text-blue-800">Check this if this municipal account receives the main bulk water/electricity meters.</p>
          </div>
          <label class="relative inline-flex items-center cursor-pointer">
            <input type="checkbox" id="wiz_acc_is_bulk" class="sr-only peer">
            <div class="w-11 h-6 bg-slate-300 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
          </label>
        </div>
"""
if 'wiz_acc_is_bulk' not in content:
    content = content.replace('<div class="grid grid-cols-1 md:grid-cols-2 gap-6">', bulk_checkbox + '<div class="grid grid-cols-1 md:grid-cols-2 gap-6">')

# Update the wizMeterTemplate
old_template = """<template id="wizMeterTemplate">
  <div class="meter-card bg-white p-5 rounded-xl border-2 shadow-sm relative mb-4">
    <button type="button" onclick="this.closest('.meter-card').remove()" class="absolute top-4 right-4 text-slate-400 hover:text-red-500 transition">
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
    </button>
    
    <div class="flex items-center mb-4">
      <h4 class="meter-title font-bold text-lg"></h4>
    </div>
    
    <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
      <div>
        <label class="block text-xs font-semibold text-slate-600 mb-1">Meter Number</label>
        <input type="text" class="m-number w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500" required>
      </div>
      <div>
        <label class="block text-xs font-semibold text-slate-600 mb-1">Assignment</label>
        <select class="m-assign w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500">
          <option value="linked">Linked (Sub-Unit)</option>
          <option value="bulk">Bulk Supply (Main)</option>
        </select>
      </div>
      <div>
        <label class="block text-xs font-semibold text-slate-600 mb-1">Status</label>
        <select class="m-status w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500" onchange="handleStolenToggle(this)">
          <option value="active">Active</option>
          <option value="stolen">Stolen</option>
        </select>
      </div>
    </div>

    <!-- Hidden block for stolen meters -->
    <div class="stolen-action hidden mt-4 pt-4 border-t border-slate-200">
      <div class="bg-amber-50 border border-amber-200 p-4 rounded-lg">
        <h5 class="text-amber-900 font-bold text-sm mb-2 flex items-center">
          <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>
          Stolen Meter Action
        </h5>
        <label class="block text-xs font-semibold text-amber-800 mb-1">Has this meter been physically replaced?</label>
        <select class="w-full rounded border border-amber-300 px-3 py-2 text-sm outline-none focus:border-amber-500 bg-white" onchange="handleReplacementToggle(this)">
          <option value="no">No</option>
          <option value="yes">Yes</option>
        </select>
        <div class="replacement-msg hidden mt-3 text-xs text-amber-700">
          * Add the new replacement meter as a separate meter block below, and mark it Active.
        </div>
      </div>
    </div>
  </div>
</template>"""

new_template = """<template id="wizMeterTemplate">
  <div class="meter-card bg-white p-5 rounded-xl border-2 shadow-sm relative mb-4 transition">
    <button type="button" onclick="this.closest('.meter-card').remove()" class="absolute top-4 right-4 text-slate-400 hover:text-red-500">
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
    </button>
    
    <div class="flex items-center mb-4">
      <h4 class="meter-title font-bold text-lg"></h4>
    </div>
    
    <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
      <div>
        <label class="block text-xs font-semibold text-slate-600 mb-1">Meter Number</label>
        <input type="text" class="m-number w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500" required>
        <input type="hidden" class="m-replacement">
      </div>
      <div>
        <label class="block text-xs font-semibold text-slate-600 mb-1">Meter Source / Link</label>
        <select class="m-assign w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500">
          <!-- Populated dynamically -->
        </select>
      </div>
      <div>
        <label class="block text-xs font-semibold text-slate-600 mb-1">Status</label>
        <select class="m-status w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500">
          <option value="active">Active / Normal</option>
          <option value="stolen">Stolen (Municipal Record)</option>
          <option value="new_physical">New Physical (Replacement)</option>
        </select>
      </div>
    </div>
  </div>
</template>"""

if 'wizMeterTemplate' in content:
    content = content.replace(old_template, new_template)

# Add Exceptional Case button
old_buttons = """          <div class="flex space-x-4">
            <button type="button" onclick="addWizardMeter('water')" class="flex-1 bg-sky-50 hover:bg-sky-100 border border-sky-200 text-sky-700 font-bold py-3 rounded-xl shadow-sm transition flex justify-center items-center">
              <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"></path></svg>
              + Add Water Meter
            </button>
            <button type="button" onclick="addWizardMeter('elec')" class="flex-1 bg-indigo-50 hover:bg-indigo-100 border border-indigo-200 text-indigo-700 font-bold py-3 rounded-xl shadow-sm transition flex justify-center items-center">
              <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
              + Add Elec Meter
            </button>
          </div>"""

new_buttons = """          <div class="flex space-x-4">
            <button type="button" onclick="addNormalMeter('water')" class="flex-1 bg-sky-50 hover:bg-sky-100 border border-sky-200 text-sky-700 font-bold py-3 rounded-xl shadow-sm transition flex justify-center items-center">
              <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"></path></svg>
              + Add Water Meter
            </button>
            <button type="button" onclick="addNormalMeter('elec')" class="flex-1 bg-indigo-50 hover:bg-indigo-100 border border-indigo-200 text-indigo-700 font-bold py-3 rounded-xl shadow-sm transition flex justify-center items-center">
              <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
              + Add Elec Meter
            </button>
          </div>
          <button type="button" onclick="addExceptionalCase()" class="w-full flex items-center justify-center bg-red-50 hover:bg-red-100 text-red-700 font-bold py-3 px-4 rounded-xl border border-red-200 shadow-sm transition mt-4">
            <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>
            + Add Exceptional Case (Stolen/Replaced)
          </button>"""
if '+ Add Exceptional Case' not in content:
    content = content.replace(old_buttons, new_buttons)

# Update Javascript Functions
old_js = """  function openWizardModal(accountId) {
    document.getElementById('wizardModal').classList.remove('hidden');
    switchTab('tab-main');
    
    // Reset form
    document.getElementById('wizardForm').reset();
    document.getElementById('wiz_meters_container').innerHTML = '';
    
    if (accountId === 'new') {
      document.getElementById('wizardTitle').textContent = 'New Account Setup';
      document.getElementById('wiz_account_id').value = '';
    } else {
      document.getElementById('wizardTitle').textContent = 'Edit Account Setup';
      document.getElementById('wiz_account_id').value = accountId;
      // In a real app, we'd fetch account data via AJAX and populate the form here.
      // For now, if editing, we just rely on server reload to show changes.
    }
  }

  function closeWizardModal() {
    document.getElementById('wizardModal').classList.add('hidden');
  }

  function addWizardMeter(type) {
    const template = document.getElementById('wizMeterTemplate').content.cloneNode(true);
    const card = template.querySelector('.meter-card');
    card.dataset.type = type;
    
    if (type === 'water') {
      card.classList.add('border-sky-300');
      card.querySelector('.meter-title').textContent = 'Water Meter';
      card.querySelector('.meter-title').classList.add('text-sky-800');
    } else {
      card.classList.add('border-indigo-300');
      card.querySelector('.meter-title').textContent = 'Elec Meter';
      card.querySelector('.meter-title').classList.add('text-indigo-800');
    }
    
    document.getElementById('wiz_meters_container').appendChild(card);
  }

  function handleStolenToggle(selectEl) {
    const stolenActionDiv = selectEl.closest('.meter-card').querySelector('.stolen-action');
    if (selectEl.value === 'stolen') {
      stolenActionDiv.classList.remove('hidden');
    } else {
      stolenActionDiv.classList.add('hidden');
    }
  }

  function handleReplacementToggle(selectEl) {
    const msgDiv = selectEl.closest('.stolen-action').querySelector('.replacement-msg');
    if (selectEl.value === 'yes') {
      msgDiv.classList.remove('hidden');
    } else {
      msgDiv.classList.add('hidden');
    }
  }

  async function saveAccount(e) {"""

new_js = """  const map = {};
  {% for acc in accounts %}
  map[{{ acc.id }}] = {
    id: {{ acc.id }},
    account_number: "{{ acc.account_number or '' }}",
    owner_name: "{{ acc.owner.name if acc.owner else '' }}",
    is_bulk_account: {% if acc.is_bulk_account %}true{% else %}false{% endif %}
  };
  {% endfor %}

  function openWizardModal(accountId) {
    document.getElementById('wizardModal').classList.remove('hidden');
    switchTab('tab-main');
    
    document.getElementById('wizardForm').reset();
    document.getElementById('wiz_meters_container').innerHTML = '';
    
    if (accountId === 'new') {
      document.getElementById('wizardTitle').textContent = 'New Account Setup';
      document.getElementById('wiz_account_id').value = '';
    } else {
      document.getElementById('wizardTitle').textContent = 'Edit Account Setup';
      document.getElementById('wiz_account_id').value = accountId;
      const acc = map[accountId];
      if(acc) {
         document.getElementById('wiz_acc_number').value = acc.account_number;
         document.getElementById('wiz_acc_owner').value = acc.owner_name;
         document.getElementById('wiz_acc_is_bulk').checked = acc.is_bulk_account;
      }
    }
  }

  function closeWizardModal() {
    document.getElementById('wizardModal').classList.add('hidden');
  }

  function addWizardMeter(type, isExceptional=false, isStolen=false) {
    const template = document.getElementById('wizMeterTemplate').content.cloneNode(true);
    const card = template.querySelector('.meter-card');
    card.dataset.type = type;
    
    if (type === 'water') {
      card.classList.add('border-sky-300');
      card.querySelector('.meter-title').textContent = 'Water Meter';
      card.querySelector('.meter-title').classList.add('text-sky-800');
    } else {
      card.classList.add('border-indigo-300');
      card.querySelector('.meter-title').textContent = 'Elec Meter';
      card.querySelector('.meter-title').classList.add('text-indigo-800');
    }

    // Populate assignment options dynamically
    const isBulk = document.getElementById('wiz_acc_is_bulk').checked;
    const assignSelect = card.querySelector('.m-assign');
    assignSelect.innerHTML = '';
    
    if(isExceptional && isStolen) {
      assignSelect.innerHTML = '<option value="stolen_exception">Stolen / Exceptional Link</option>';
    } else if (isBulk) {
      assignSelect.innerHTML = `<option value="bulk_supply">Bulk Supply (This is a main meter)</option><option value="independent">Independent (Not bulk)</option>`;
    } else {
      let opts = `<option value="independent">Independent Meter</option>`;
      const relevantBulk = bulkMeters.filter(m => m.type === type);
      if(relevantBulk.length > 0) {
        opts += `<optgroup label="Link to Bulk Meter">`;
        relevantBulk.forEach(m => {
          opts += `<option value="linked_bulk_${m.id}">Linked to Bulk: #${m.number}</option>`;
        });
        opts += `</optgroup>`;
      }
      assignSelect.innerHTML = opts;
    }
    
    return card;
  }
  
  function addNormalMeter(type) {
     const card = addWizardMeter(type, false, false);
     document.getElementById('wiz_meters_container').appendChild(card);
  }

  function addExceptionalCase() {
    const container = document.getElementById('wiz_meters_container');
    const wrapper = document.createElement('div');
    wrapper.className = "border-2 border-red-200 bg-red-50 p-4 rounded-xl mb-4 relative";
    wrapper.innerHTML = `
      <h3 class="font-bold text-red-800 mb-2">Exceptional Case: Stolen / Replaced Meter</h3>
      <p class="text-xs text-red-600 mb-4">Link the stolen municipal meter to the new physical replacement meter.</p>
      <button type="button" onclick="this.parentElement.remove()" class="absolute top-4 right-4 text-red-400 hover:text-red-600 font-bold">X Remove</button>
      <div class="exception-cards"></div>
    `;

    // Stolen card (No bulk mapping)
    const stolenCard = addWizardMeter('water', true, true);
    stolenCard.classList.replace('bg-white', 'bg-red-100');
    stolenCard.querySelector('.meter-title').textContent = "1. Stolen Municipal Meter";
    stolenCard.querySelector('.m-status').value = 'stolen';
    
    // Replacement card (CAN link to bulk meter!)
    const repCard = addWizardMeter('water', true, false);
    repCard.classList.replace('bg-white', 'bg-green-50');
    repCard.querySelector('.meter-title').textContent = "2. New Physical Meter";
    repCard.querySelector('.m-status').value = 'new_physical';

    stolenCard.querySelector('.m-number').addEventListener('input', (e) => {
      repCard.querySelector('.m-replacement').value = e.target.value;
    });

    wrapper.querySelector('.exception-cards').appendChild(stolenCard);
    wrapper.querySelector('.exception-cards').appendChild(repCard);
    container.appendChild(wrapper);
  }

  async function saveAccount(e) {"""

if 'function addNormalMeter(type)' not in content:
    content = content.replace(old_js, new_js)

old_save_form = """    const formData = new FormData(form);
    
    try {"""
new_save_form = """    const formData = new FormData(form);
    formData.append('is_bulk_account', document.getElementById('wiz_acc_is_bulk').checked);
    
    try {"""
if "formData.append('is_bulk_account'" not in content:
    content = content.replace(old_save_form, new_save_form)

old_meter_pack = """      meters.push({
        utility_type: card.dataset.type,
        meter_number: card.querySelector('.m-number').value,
        assignment: card.querySelector('.m-assign').value,
        status: card.querySelector('.m-status').value,
// No dates packed
        // No readings packed during Phase 1
      });"""

new_meter_pack = """      meters.push({
        utility_type: card.dataset.type,
        meter_number: card.querySelector('.m-number').value,
        assignment: card.querySelector('.m-assign').value,
        status: card.querySelector('.m-status').value,
        replacement_for: card.querySelector('.m-replacement').value || ''
      });"""
if "replacement_for:" not in content:
    content = content.replace(old_meter_pack, new_meter_pack)

with open('templates/program_billing/manual_capture.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Finally, fully rebuilt manual_capture.html for relational UI!")
