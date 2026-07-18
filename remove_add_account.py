import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove the "Add Another Account" button
old_add_btn = """          <button type="button" onclick="addAccountRow()" class="mt-4 text-sm font-bold text-blue-600 hover:text-blue-800 flex items-center">
            <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"></path></svg>
            Add Another Account
          </button>"""
content = content.replace(old_add_btn, "")

# 2. Update addAccountRow() in javascript to remove the delete button
old_row_html = """      <div class="col-span-2 flex justify-center items-center space-x-3">
        <input type="radio" name="bulk_idx" value="${idx}" class="w-4 h-4 text-blue-600 focus:ring-blue-500 cursor-pointer">
        <button type="button" onclick="this.closest('.acc-row').remove()" class="text-slate-400 hover:text-red-500"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg></button>
      </div>"""

new_row_html = """      <div class="col-span-2 flex justify-center items-center">
        <input type="radio" name="bulk_idx" value="${idx}" class="w-4 h-4 text-blue-600 focus:ring-blue-500 cursor-pointer">
      </div>"""

content = content.replace(old_row_html, new_row_html)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Removed Add Account button and delete icons.")
