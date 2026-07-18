import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the initialization JS block with one that preloads data
old_init = """  // Initialization
  document.addEventListener('DOMContentLoaded', () => {
    // Generate initial rows based on expected bills (default to 8 for demo purposes, or pull from template)
    const expected = {{ property.expected_bills|default(8) }};
    for(let i=0; i<expected; i++) {
      addAccountRow();
    }
    // Set first one as bulk by default
    const firstRadio = document.querySelector('input[name="bulk_idx"]');
    if(firstRadio) firstRadio.checked = true;
  });"""

new_init = """  // Initialization & Preloading
  document.addEventListener('DOMContentLoaded', () => {
    // Check if we have existing accounts in the database
    {% if accounts %}
      let accIdx = 0;
      {% for acc in accounts %}
        addAccountRow("{{ acc.account_number or '' }}", "{{ acc.owner.name if acc.owner else '' }}");
        if ({{ 'true' if acc.is_bulk_account else 'false' }}) {
          const radio = document.querySelector(`input[name="bulk_idx"][value="${accIdx}"]`);
          if(radio) radio.checked = true;
        }
        accIdx++;
      {% endfor %}
      
      // Load Existing Meters
      {% for m in all_meters %}
        {% if m.status != 'stolen' %}
          if("{{ m.utility_type }}" === "water") {
            addMeterRow('water', "{{ m.meter_number }}");
          } else if("{{ m.utility_type }}" === "electricity" || "{{ m.utility_type }}" === "elec") {
            addMeterRow('elec', "{{ m.meter_number }}");
          }
        {% endif %}
      {% endfor %}
      
      // If no meters existed, maybe add 1 empty row just to start
      if(document.getElementById('water-meters-container').children.length === 0) addMeterRow('water');
      if(document.getElementById('elec-meters-container').children.length === 0) addMeterRow('elec');

    {% else %}
      // Generate initial blank rows based on expected bills
      const expected = {{ property.expected_bills|default(8) }};
      for(let i=0; i<expected; i++) {
        addAccountRow();
      }
      // Set first one as bulk by default
      const firstRadio = document.querySelector('input[name="bulk_idx"]');
      if(firstRadio) firstRadio.checked = true;
      
      // Add one blank meter row to start
      addMeterRow('water');
      addMeterRow('elec');
    {% endif %}
  });"""
content = content.replace(old_init, new_init)

# We also need to update addAccountRow and addMeterRow to accept prefilled values
old_add_acc = """  function addAccountRow() {
    const container = document.getElementById('accounts-container');
    const idx = container.children.length;
    const row = document.createElement('div');
    row.className = "grid grid-cols-12 gap-4 items-center bg-white p-2 rounded-lg border border-slate-200 shadow-sm acc-row";
    row.innerHTML = `
      <div class="col-span-5">
        <input type="text" class="acc-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Account Number">
      </div>
      <div class="col-span-5">
        <input type="text" class="acc-owner w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Owner Name">
      </div>
      <div class="col-span-2 flex justify-center items-center">
        <input type="radio" name="bulk_idx" value="${idx}" class="w-4 h-4 text-blue-600 focus:ring-blue-500 cursor-pointer">
      </div>
    `;
    container.appendChild(row);
  }"""
new_add_acc = """  function addAccountRow(accNum="", ownerName="") {
    const container = document.getElementById('accounts-container');
    const idx = container.children.length;
    const row = document.createElement('div');
    row.className = "grid grid-cols-12 gap-4 items-center bg-white p-2 rounded-lg border border-slate-200 shadow-sm acc-row";
    row.innerHTML = `
      <div class="col-span-5">
        <input type="text" class="acc-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Account Number" value="${accNum}">
      </div>
      <div class="col-span-5">
        <input type="text" class="acc-owner w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Owner Name" value="${ownerName}">
      </div>
      <div class="col-span-2 flex justify-center items-center">
        <input type="radio" name="bulk_idx" value="${idx}" class="w-4 h-4 text-blue-600 focus:ring-blue-500 cursor-pointer">
      </div>
    `;
    container.appendChild(row);
  }"""
content = content.replace(old_add_acc, new_add_acc)

old_add_meter = """  function addMeterRow(type) {
    const container = document.getElementById(type === 'water' ? 'water-meters-container' : 'elec-meters-container');
    const row = document.createElement('div');
    row.className = "flex items-center space-x-3 bg-white p-2 rounded-lg border border-slate-200 shadow-sm meter-row";
    row.innerHTML = `
      <div class="flex-grow">
        <input type="text" class="meter-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Meter Number">
      </div>
      <button type="button" onclick="this.closest('.meter-row').remove()" class="text-slate-400 hover:text-red-500 px-2"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg></button>
    `;
    container.appendChild(row);
  }"""
new_add_meter = """  function addMeterRow(type, val="") {
    const container = document.getElementById(type === 'water' ? 'water-meters-container' : 'elec-meters-container');
    const row = document.createElement('div');
    row.className = "flex items-center space-x-3 bg-white p-2 rounded-lg border border-slate-200 shadow-sm meter-row";
    row.innerHTML = `
      <div class="flex-grow">
        <input type="text" class="meter-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Meter Number" value="${val}">
      </div>
      <button type="button" onclick="this.closest('.meter-row').remove()" class="text-slate-400 hover:text-red-500 px-2"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg></button>
    `;
    container.appendChild(row);
  }"""
content = content.replace(old_add_meter, new_add_meter)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated JS preloading")
