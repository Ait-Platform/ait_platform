import re

#########################################
# 1. Update manager_dashboard.html
#########################################
with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    dashboard = f.read()

# The modal form contains:
# <div class="mb-4">
#   <label class="block text-sm font-bold text-slate-700 mb-2">How many Bill Account Number(s) are required?</label>
#   <input type="number" name="bills" ...>
# </div>
# ... up to sub_meters

# We can replace everything from the bills div to the end of the form with just the submit buttons
pattern = r'<div class="mb-4">\s*<label class="block text-sm font-bold text-slate-700 mb-2">How many Bill Account Number\(s\) are required\?</label>.*?<div class="flex justify-end space-x-3 pt-6">'

replacement = '''<div class="flex justify-end space-x-3 pt-6">'''
dashboard = re.sub(pattern, replacement, dashboard, flags=re.DOTALL)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(dashboard)


#########################################
# 2. Update routes.py
#########################################
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes = f.read()

old_params = '''    bills = int(request.form.get("bills", 1))
    tenants = int(request.form.get("tenants", 1))
    is_bulk = request.form.get("is_bulk", "no")
    sub_meters = int(request.form.get("sub_meters", 0))'''

new_params = '''    bills = 1
    tenants = 1
    is_bulk = "no"
    sub_meters = 0'''

routes = routes.replace(old_params, new_params)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes)


#########################################
# 3. Update setup_wizard.html
#########################################
with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    wizard = f.read()

# Change addAccountRow back to ALWAYS generating the bulk column
old_add_row = '''  const isBulkEnabled = {{ 'true' if property.is_bulk_metered else 'false' }};
  
  function addAccountRow(accNum="", ownerName="", isBulk=false) {
    const container = document.getElementById('accounts-container');
    const idx = container.children.length;
    const rowColor = (idx % 2 === 0) ? 'bg-sky-50' : 'bg-white';
    const row = document.createElement('div');
    row.className = `grid grid-cols-12 gap-4 items-center p-2 rounded-lg border border-slate-200 shadow-sm acc-row ${rowColor}`;
    
    let innerHTML = `
      <div class="col-span-${isBulkEnabled ? '5' : '6'}">
        <input type="text" class="acc-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Account Number" value="${accNum}">
      </div>
      <div class="col-span-${isBulkEnabled ? '5' : '6'}">
        <input type="text" class="acc-owner w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Owner Name" value="${ownerName}">
      </div>
    `;
    
    if (isBulkEnabled) {
      innerHTML += `
        <div class="col-span-2 flex justify-center items-center">
          <input type="radio" name="bulk_idx" value="${idx}" class="w-4 h-4 text-blue-600 focus:ring-blue-500 cursor-pointer" ${isBulk ? 'checked' : ''}>
        </div>
      `;
    }
    
    row.innerHTML = innerHTML;
    container.appendChild(row);
  }'''

new_add_row = '''  function addAccountRow(accNum="", ownerName="", isBulk=false) {
    const container = document.getElementById('accounts-container');
    const idx = container.children.length;
    const rowColor = (idx % 2 === 0) ? 'bg-sky-50' : 'bg-white';
    const row = document.createElement('div');
    row.className = `grid grid-cols-12 gap-4 items-center p-2 rounded-lg border border-slate-200 shadow-sm acc-row ${rowColor}`;
    row.innerHTML = `
      <div class="col-span-6 bulk-col-adj">
        <input type="text" class="acc-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Account Number" value="${accNum}">
      </div>
      <div class="col-span-6 bulk-col-adj">
        <input type="text" class="acc-owner w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Owner Name" value="${ownerName}">
      </div>
      <div class="col-span-2 flex justify-center items-center bulk-col hidden">
        <input type="radio" name="bulk_idx" value="${idx}" class="w-4 h-4 text-blue-600 focus:ring-blue-500 cursor-pointer" ${isBulk ? 'checked' : ''}>
      </div>
    `;
    container.appendChild(row);
  }'''
wizard = wizard.replace(old_add_row, new_add_row)

# Change header
old_header = '''          <div class="grid grid-cols-12 gap-4 text-xs font-bold text-slate-500 uppercase tracking-wider mb-2 px-2">
            <div class="col-span-{% if property.is_bulk_metered %}5{% else %}6{% endif %}">Account Number</div>
            <div class="col-span-{% if property.is_bulk_metered %}5{% else %}6{% endif %}">Owner Name</div>
            {% if property.is_bulk_metered %}
            <div class="col-span-2 text-center">Is Bulk?</div>
            {% endif %}
          </div>'''
new_header = '''          <div class="grid grid-cols-12 gap-4 text-xs font-bold text-slate-500 uppercase tracking-wider mb-2 px-2">
            <div class="col-span-6 bulk-col-adj">Account Number</div>
            <div class="col-span-6 bulk-col-adj">Owner Name</div>
            <div class="col-span-2 text-center bulk-col hidden">Is Bulk?</div>
          </div>'''
wizard = wizard.replace(old_header, new_header)

# Add CSS toggle logic
js_logic = '''
  function updateBulkVisibility() {
      const bw = document.getElementById('pm_bulk_water').value === 'yes';
      const be = document.getElementById('pm_bulk_elec').value === 'yes';
      const showBulk = bw || be;
      
      document.querySelectorAll('.bulk-col').forEach(el => {
          if (showBulk) el.classList.remove('hidden');
          else el.classList.add('hidden');
      });
      document.querySelectorAll('.bulk-col-adj').forEach(el => {
          if (showBulk) {
              el.classList.remove('col-span-6');
              el.classList.add('col-span-5');
          } else {
              el.classList.remove('col-span-5');
              el.classList.add('col-span-6');
          }
      });
  }
  
  // Attach to event listeners
  document.getElementById('pm_bulk_water').addEventListener('change', updateBulkVisibility);
  document.getElementById('pm_bulk_elec').addEventListener('change', updateBulkVisibility);
'''
if 'updateBulkVisibility' not in wizard:
    wizard = wizard.replace("function readPropertyMap() {", js_logic + "\n  function readPropertyMap() {")

# Ensure it's called on init
if 'updateBulkVisibility();' not in wizard:
    wizard = wizard.replace("recalculateRows();", "recalculateRows();\n      updateBulkVisibility();")

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(wizard)

print("Updated all files.")
