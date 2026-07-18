with open('templates/program_billing/manual_capture.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# 1. Add "Is Bulk Account" checkbox to Tab 1
bulk_html = """        <div class="mb-4 bg-blue-50 border border-blue-200 p-4 rounded-lg flex items-center justify-between">
          <div>
            <h4 class="font-bold text-blue-900 text-sm">Designate as Bulk Account?</h4>
            <p class="text-xs text-blue-800">Check this if this municipal account receives the main bulk water/electricity meters.</p>
          </div>
          <label class="relative inline-flex items-center cursor-pointer">
            <input type="checkbox" id="acc-is-bulk" class="sr-only peer">
            <div class="w-11 h-6 bg-slate-300 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
          </label>
        </div>
        """
content = content.replace('<div class="grid grid-cols-1 md:grid-cols-2 gap-6">', bulk_html + '<div class="grid grid-cols-1 md:grid-cols-2 gap-6">')

# 2. Inject bulk_meters into JS
js_inject = """<script>
  const bulkMeters = [
    {% for bm in bulk_meters %}
    { id: {{ bm.id }}, number: "{{ bm.meter_number }}", type: "{{ bm.utility_type }}" }{% if not loop.last %},{% endif %}
    {% endfor %}
  ];
"""
content = content.replace("<script>", js_inject)

# 3. Update Meter card template
old_template = """<template id="meter-template">
  <div class="meter-card bg-white p-5 rounded-xl border border-slate-200 shadow-sm relative">
    <button type="button" onclick="this.closest('.meter-card').remove()" class="absolute top-4 right-4 text-slate-400 hover:text-red-500">
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
    </button>
    
    <div class="flex items-center mb-4">
      <span class="m-icon w-8 h-8 rounded-full flex items-center justify-center mr-3"></span>
      <h4 class="m-title font-bold text-slate-800"></h4>
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
        <select class="m-status w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500">
          <option value="active">Active</option>
          <option value="stolen">Stolen</option>
          <option value="replaced">Replaced</option>
        </select>
      </div>
    </div>
  </div>
</template>"""

new_template = """<template id="meter-template">
  <div class="meter-card bg-white p-5 rounded-xl border border-slate-200 shadow-sm relative mb-4 transition">
    <button type="button" onclick="this.closest('.meter-card').remove()" class="absolute top-4 right-4 text-slate-400 hover:text-red-500">
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
    </button>
    
    <div class="flex items-center mb-4">
      <span class="m-icon w-8 h-8 rounded-full flex items-center justify-center mr-3"></span>
      <h4 class="m-title font-bold text-slate-800"></h4>
    </div>
    
    <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
      <div>
        <label class="block text-xs font-semibold text-slate-600 mb-1">Meter Number</label>
        <input type="text" class="m-number w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500" required>
        <!-- Hidden field for stolen replacement mapping -->
        <input type="hidden" class="m-replacement">
      </div>
      <div>
        <label class="block text-xs font-semibold text-slate-600 mb-1">Meter Source / Link</label>
        <select class="m-assign w-full rounded border border-slate-300 px-3 py-2 text-sm outline-none focus:border-emerald-500">
          <!-- Options populated by JS -->
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

content = content.replace(old_template, new_template)

# 4. Update the Javascript addMeter logic to populate the m-assign dropdown correctly
old_add_meter = """  function addMeter(type) {
    const tpl = document.getElementById('meter-template');
    const clone = tpl.content.cloneNode(true);
    const card = clone.querySelector('.meter-card');
    
    card.dataset.type = type;
    const title = clone.querySelector('.m-title');
    const icon = clone.querySelector('.m-icon');
    
    if(type === 'water') {
      title.textContent = 'Water Meter';
      icon.classList.add('bg-sky-100', 'text-sky-600');
      icon.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"></path></svg>';
    } else {
      title.textContent = 'Electricity Meter';
      icon.classList.add('bg-indigo-100', 'text-indigo-600');
      icon.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>';
    }
    
    document.getElementById('meters-container').appendChild(clone);
  }"""

new_add_meter = """  function addMeter(type, isExceptional = false) {
    const tpl = document.getElementById('meter-template');
    const clone = tpl.content.cloneNode(true);
    const card = clone.querySelector('.meter-card');
    
    card.dataset.type = type;
    const title = clone.querySelector('.m-title');
    const icon = clone.querySelector('.m-icon');
    
    if(type === 'water') {
      title.textContent = 'Water Meter';
      icon.classList.add('bg-sky-100', 'text-sky-600');
      icon.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"></path></svg>';
    } else {
      title.textContent = 'Electricity Meter';
      icon.classList.add('bg-indigo-100', 'text-indigo-600');
      icon.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>';
    }

    // Populate the dropdown logic based on Bulk toggle
    const isBulk = document.getElementById('acc-is-bulk').checked;
    const assignSelect = card.querySelector('.m-assign');
    assignSelect.innerHTML = ''; // clear

    if(isExceptional) {
      assignSelect.innerHTML = '<option value="stolen_exception">Stolen / Exceptional Link</option>';
    } else if (isBulk) {
      assignSelect.innerHTML = `
        <option value="bulk_supply">Bulk Supply (This is a main meter)</option>
        <option value="independent">Independent (Not bulk)</option>
      `;
    } else {
      let options = `<option value="independent">Independent Meter</option>`;
      const relevantBulk = bulkMeters.filter(m => m.type === type);
      if(relevantBulk.length > 0) {
        options += `<optgroup label="Link to Bulk Meter">`;
        relevantBulk.forEach(m => {
          options += `<option value="linked_bulk_${m.id}">Linked to: #${m.number}</option>`;
        });
        options += `</optgroup>`;
      }
      assignSelect.innerHTML = options;
    }
    
    return card;
  }
  
  function addNormalMeter(type) {
    const card = addMeter(type);
    document.getElementById('meters-container').appendChild(card);
  }

  function addExceptionalCase() {
    // Adds a dual-block for Stolen and Replaced
    const container = document.getElementById('meters-container');
    
    const wrapper = document.createElement('div');
    wrapper.className = "border-2 border-red-200 bg-red-50 p-4 rounded-xl mb-4 relative";
    wrapper.innerHTML = `
      <h3 class="font-bold text-red-800 mb-2">Exceptional Case: Stolen / Replaced Meter</h3>
      <p class="text-xs text-red-600 mb-4">Link the municipal stolen meter to your new physical tenant meter.</p>
      <button type="button" onclick="this.parentElement.remove()" class="absolute top-4 right-4 text-red-400 hover:text-red-600">Remove Exception</button>
      <div class="exception-cards"></div>
    `;

    // Stolen card
    const stolenCard = addMeter('water', true);
    stolenCard.classList.replace('bg-white', 'bg-red-100');
    stolenCard.querySelector('.m-title').textContent = "1. Stolen Municipal Meter";
    stolenCard.querySelector('.m-status').value = 'stolen';
    
    // Replacement card
    const repCard = addMeter('water', true);
    repCard.classList.replace('bg-white', 'bg-green-50');
    repCard.querySelector('.m-title').textContent = "2. New Physical Meter";
    repCard.querySelector('.m-status').value = 'new_physical';

    // When the stolen meter number is typed, update the replacement's hidden field to link them
    stolenCard.querySelector('.m-number').addEventListener('input', (e) => {
      repCard.querySelector('.m-replacement').value = e.target.value;
    });

    wrapper.querySelector('.exception-cards').appendChild(stolenCard);
    wrapper.querySelector('.exception-cards').appendChild(repCard);
    container.appendChild(wrapper);
  }
"""
content = content.replace(old_add_meter, new_add_meter)

# 5. Replace HTML onClick handlers
content = content.replace("onclick=\"addMeter('water')\"", "onclick=\"addNormalMeter('water')\"")
content = content.replace("onclick=\"addMeter('elec')\"", "onclick=\"addNormalMeter('elec')\"")

# Add the Exceptional Case button
exception_btn = """<button type="button" onclick="addExceptionalCase()" class="w-full flex items-center justify-center bg-red-50 hover:bg-red-100 text-red-700 font-bold py-3 px-4 rounded-xl border border-red-200 shadow-sm transition">
          <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>
          Add Exceptional Case (Stolen/Replaced)
        </button>"""
content = content.replace('</div>\n      </div>\n      \n      <!-- Meters List -->', exception_btn + '\n      </div>\n      </div>\n      \n      <!-- Meters List -->')

# 6. Update openAccount to load is_bulk_account correctly
old_openAccount = """    document.getElementById('acc-owner').value = acc.owner_name;"""
new_openAccount = """    document.getElementById('acc-owner').value = acc.owner_name;
    document.getElementById('acc-is-bulk').checked = acc.is_bulk_account;"""
content = content.replace(old_openAccount, new_openAccount)

# 7. Update Python accounts map creation inside JS
old_accs_js = """    const map = {};
    {% for acc in accounts %}
    map[{{ acc.id }}] = {
      id: {{ acc.id }},
      account_number: "{{ acc.account_number or '' }}",
      owner_name: "{{ acc.owner.name if acc.owner else '' }}"
    };
    {% endfor %}"""
new_accs_js = """    const map = {};
    {% for acc in accounts %}
    map[{{ acc.id }}] = {
      id: {{ acc.id }},
      account_number: "{{ acc.account_number or '' }}",
      owner_name: "{{ acc.owner.name if acc.owner else '' }}",
      is_bulk_account: {% if acc.is_bulk_account %}true{% else %}false{% endif %}
    };
    {% endfor %}"""
content = content.replace(old_accs_js, new_accs_js)

# 8. Update saveAccount to grab is_bulk_account and replacement link
old_save_js = """  async function saveAccount(e) {
    e.preventDefault();
    const accId = document.getElementById('acc-id').value;
    const accNum = document.getElementById('acc-number').value;
    const ownerName = document.getElementById('acc-owner').value;"""

new_save_js = """  async function saveAccount(e) {
    e.preventDefault();
    const accId = document.getElementById('acc-id').value;
    const accNum = document.getElementById('acc-number').value;
    const ownerName = document.getElementById('acc-owner').value;
    const isBulk = document.getElementById('acc-is-bulk').checked;"""
content = content.replace(old_save_js, new_save_js)

old_fd_append = """    fd.append('account_id', accId);
    fd.append('account_number', accNum);
    fd.append('owner_name', ownerName);"""
new_fd_append = """    fd.append('account_id', accId);
    fd.append('account_number', accNum);
    fd.append('owner_name', ownerName);
    fd.append('is_bulk_account', isBulk);"""
content = content.replace(old_fd_append, new_fd_append)

old_meters_push = """        status: card.querySelector('.m-status').value
        // No dates or readings packed during Phase 1
      });"""
new_meters_push = """        status: card.querySelector('.m-status').value,
        replacement_for: card.querySelector('.m-replacement').value
      });"""
content = content.replace(old_meters_push, new_meters_push)

with open('templates/program_billing/manual_capture.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated manual_capture.html UI.")
