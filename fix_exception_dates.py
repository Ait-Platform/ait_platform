import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace addExceptionRow using a regex that captures the entire body of the function
# We only want to replace the HTML inside the row.innerHTML assignment and the function signature.

old_func_sig = 'function addExceptionRow(stolenNum="", repId="") {'
new_func_sig = 'function addExceptionRow(stolenNum="", repId="", dateStolen="", dateReplaced="") {'
content = content.replace(old_func_sig, new_func_sig)

# Replace the innerHTML block
import re
pattern = re.compile(r'(row\.innerHTML = `).*?(`;)', re.DOTALL)

new_inner_html = r"""\1
      <div class="flex-1">
        <label class="block text-[10px] font-bold text-rose-800 uppercase mb-1">Stolen Municipal Meter No.</label>
        <input type="text" class="exc-stolen-num w-full rounded border-rose-300 px-2 py-1.5 text-xs outline-none" placeholder="e.g. CEL884" value="${stolenNum}">
      </div>
      <div class="w-32">
        <label class="block text-[10px] font-bold text-rose-800 uppercase mb-1">Date Stolen</label>
        <input type="date" class="exc-date-stolen w-full rounded border-rose-300 px-2 py-1.5 text-xs outline-none" value="${dateStolen}">
      </div>
      <div class="flex items-center pt-5 px-2 hidden md:block">
        <svg class="w-5 h-5 text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3"></path></svg>
      </div>
      <div class="flex-1">
        <label class="block text-[10px] font-bold text-emerald-800 uppercase mb-1">Replaced By (Sub-Meter)</label>
        <select class="exc-replacement-id w-full rounded border-emerald-300 px-2 py-1.5 text-xs outline-none bg-white" data-selected="${repId}">
          <!-- Options populated by updateExceptionDropdowns() -->
        </select>
      </div>
      <div class="w-32">
        <label class="block text-[10px] font-bold text-emerald-800 uppercase mb-1">Date Replaced</label>
        <input type="date" class="exc-date-replaced w-full rounded border-emerald-300 px-2 py-1.5 text-xs outline-none" value="${dateReplaced}">
      </div>
      <button type="button" onclick="this.closest('.exc-row').remove(); triggerAutoSave();" class="mt-5 text-slate-400 hover:text-red-500">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
      </button>
    \2"""

# Make sure we only replace the FIRST match (which is addExceptionRow's innerHTML)
# wait, there are other row.innerHTML in the file! (like mapping)
# So let's find the exact string to replace.

start_str = 'row.innerHTML = `\n      <div class="flex-1">\n        <label class="block text-[10px] font-bold text-rose-800 uppercase mb-1">Stolen Municipal Meter No.</label>'
end_str = '    `;\n    container.appendChild(row);'

start_idx = content.find(start_str)
end_idx = content.find(end_str, start_idx)

if start_idx != -1 and end_idx != -1:
    old_block = content[start_idx:end_idx]
    
    new_block = """row.innerHTML = `
      <div class="flex-1">
        <label class="block text-[10px] font-bold text-rose-800 uppercase mb-1">Stolen Municipal Meter No.</label>
        <input type="text" class="exc-stolen-num w-full rounded border-rose-300 px-2 py-1.5 text-xs outline-none" placeholder="e.g. CEL884" value="${stolenNum}">
      </div>
      <div class="w-32">
        <label class="block text-[10px] font-bold text-rose-800 uppercase mb-1">Date Stolen</label>
        <input type="date" class="exc-date-stolen w-full rounded border-rose-300 px-2 py-1.5 text-xs outline-none" value="${dateStolen}">
      </div>
      <div class="flex items-center pt-5 px-2 hidden md:block">
        <svg class="w-5 h-5 text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3"></path></svg>
      </div>
      <div class="flex-1">
        <label class="block text-[10px] font-bold text-emerald-800 uppercase mb-1">Replaced By (Sub-Meter)</label>
        <select class="exc-replacement-id w-full rounded border-emerald-300 px-2 py-1.5 text-xs outline-none bg-white" data-selected="${repId}">
          <!-- Options populated by updateExceptionDropdowns() -->
        </select>
      </div>
      <div class="w-32">
        <label class="block text-[10px] font-bold text-emerald-800 uppercase mb-1">Date Replaced</label>
        <input type="date" class="exc-date-replaced w-full rounded border-emerald-300 px-2 py-1.5 text-xs outline-none" value="${dateReplaced}">
      </div>
      <button type="button" onclick="this.closest('.exc-row').remove(); triggerAutoSave();" class="mt-5 text-slate-400 hover:text-red-500">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
      </button>\n"""

    content = content.replace(old_block, new_block)
    
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed addExceptionRow HTML successfully.")
else:
    print("Could not find the HTML block to replace.")
