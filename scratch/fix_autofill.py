import re

with open('templates/program_practice_crm/staff.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix email input autocomplete
old_email = '<input type="email" name="email" list="available-receptionists" autocomplete="off" required placeholder="e.g. sarah@cityhealth.com" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-4 py-2 bg-white">'
new_email = '<input type="email" name="email" list="available-receptionists" autocomplete="new-password" required placeholder="e.g. sarah@cityhealth.com" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-4 py-2 bg-white">'

# Fix password input autocomplete
old_pw = '<input type="password" name="password" placeholder="Leave blank if already registered" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-4 py-2 bg-white">'
new_pw = '<input type="password" name="password" autocomplete="new-password" placeholder="Leave blank if already registered" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-4 py-2 bg-white">'

content = content.replace(old_email, new_email)
content = content.replace(old_pw, new_pw)

with open('templates/program_practice_crm/staff.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Replaced autocomplete attributes.")
