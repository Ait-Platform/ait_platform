import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update the table header
old_header = '''          <div class="grid grid-cols-12 gap-4 text-xs font-bold text-slate-500 uppercase tracking-wider mb-2 px-2">
            <div class="col-span-5">Account Number</div>
            <div class="col-span-5">Owner Name</div>
            <div class="col-span-2 text-center">Is Bulk?</div>
          </div>'''
new_header = '''          <div class="grid grid-cols-12 gap-4 text-xs font-bold text-slate-500 uppercase tracking-wider mb-2 px-2">
            <div class="col-span-{% if property.is_bulk_metered %}5{% else %}6{% endif %}">Account Number</div>
            <div class="col-span-{% if property.is_bulk_metered %}5{% else %}6{% endif %}">Owner Name</div>
            {% if property.is_bulk_metered %}
            <div class="col-span-2 text-center">Is Bulk?</div>
            {% endif %}
          </div>'''
content = content.replace(old_header, new_header)

# 2. Update addAccountRow
old_add_row = '''  function addAccountRow(accNum="", ownerName="", isBulk=false) {
    const container = document.getElementById('accounts-container');
    const idx = container.children.length;
    const rowColor = (idx % 2 === 0) ? 'bg-sky-50' : 'bg-white';
    const row = document.createElement('div');
    row.className = `grid grid-cols-12 gap-4 items-center p-2 rounded-lg border border-slate-200 shadow-sm acc-row ${rowColor}`;
    row.innerHTML = `
      <div class="col-span-5">
        <input type="text" class="acc-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Account Number" value="${accNum}">
      </div>
      <div class="col-span-5">
        <input type="text" class="acc-owner w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Owner Name" value="${ownerName}">
      </div>
      <div class="col-span-2 flex justify-center items-center">
        <input type="radio" name="bulk_idx" value="${idx}" class="w-4 h-4 text-blue-600 focus:ring-blue-500 cursor-pointer" ${isBulk ? 'checked' : ''}>
      </div>
    `;
    container.appendChild(row);
  }'''

new_add_row = '''  const isBulkEnabled = {{ 'true' if property.is_bulk_metered else 'false' }};
  
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
content = content.replace(old_add_row, new_add_row)

# 3. Update gatherAccounts
old_gather = '''      const isBulk = row.querySelector('input[name="bulk_idx"]').checked;'''
new_gather = '''      const bulkRadio = row.querySelector('input[name="bulk_idx"]');
      const isBulk = bulkRadio ? bulkRadio.checked : false;'''
content = content.replace(old_gather, new_gather)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated setup_wizard.html")
