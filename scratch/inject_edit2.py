import re

with open('templates/program_practice_crm/staff.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = """                  <div class="flex-1 min-w-[200px]">
                    <label class="block text-sm font-bold text-slate-700 mb-1">Phone Number</label>
                    <input type="text" id="phone-input-{{ pu.id }}" name="phone" value="{{ pu.phone or '' }}" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-3 py-2 bg-white">
                  </div>"""

injection = target + """\n                  <div class="flex-1 min-w-[200px]">
                    <label class="block text-sm font-bold text-slate-700 mb-1">Reset Password</label>
                    <input type="password" name="password" autocomplete="new-password" placeholder="Leave blank to keep current" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-3 py-2 bg-white">
                  </div>"""

if target in content:
    content = content.replace(target, injection)
    with open('templates/program_practice_crm/staff.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Success")
else:
    print("Failed again")
