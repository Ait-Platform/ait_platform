import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Update Header
old_header = """<div class="col-span-6 bulk-col-adj">Account Number</div>
<div class="col-span-6 bulk-col-adj">Owner Name</div>"""
new_header = """<div class="col-span-12 bulk-col-adj">Account Number</div>"""
html = html.replace(old_header, new_header)

# 2. Update addAccountRow
old_add_row = """      <div class="col-span-6 bulk-col-adj">
        <input type="text" class="acc-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Account Number" value="${accNum}">
      </div>
      <div class="col-span-6 bulk-col-adj">
        <input type="text" class="acc-owner w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Owner Name" value="${ownerName}">
      </div>"""
new_add_row = """      <div class="col-span-12 bulk-col-adj">
        <input type="text" class="acc-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Account Number" value="${accNum}">
      </div>"""
html = html.replace(old_add_row, new_add_row)

# 3. Update gatherAccounts
old_gather = """      const accNum = row.querySelector('.acc-num').value.trim();
      const owner = row.querySelector('.acc-owner').value.trim();
      const bulkRadio = row.querySelector('input[name="bulk_idx"]');"""
new_gather = """      const accNum = row.querySelector('.acc-num').value.trim();
      const owner = "";
      const bulkRadio = row.querySelector('input[name="bulk_idx"]');"""
html = html.replace(old_gather, new_gather)

# 4. Update updateBulkVisibility
old_vis = """              el.classList.remove('col-span-6');
              el.classList.add('col-span-5');
          } else {
              el.classList.remove('col-span-5');
              el.classList.add('col-span-6');"""
new_vis = """              el.classList.remove('col-span-12');
              el.classList.add('col-span-10');
          } else {
              el.classList.remove('col-span-10');
              el.classList.add('col-span-12');"""
html = html.replace(old_vis, new_vis)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Removed Owner Name from Step 2 successfully.")
