import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Update addExceptionRow function signature and HTML
old_add_exception_row = """  function addExceptionRow(stolenNum="", repId="") {
    const container = document.getElementById('exceptions-container');
    const row = document.createElement('div');
    row.className = "flex items-center space-x-4 bg-white p-3 rounded-lg border border-rose-200 shadow-sm exc-row";
    
    row.innerHTML = `
      <div class="flex-1">
        <label class="block text-[10px] font-bold text-rose-800 uppercase mb-1">Stolen Municipal Meter No.</label>
        <input type="text" class="exc-stolen-num w-full rounded border-rose-300 px-2 py-1.5 text-xs outline-none" placeholder="e.g. CEL884" value="${stolenNum}">
      </div>
      <div class="flex items-center pt-5 px-2">
        <svg class="w-5 h-5 text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3"></path></svg>
      </div>
      <div class="flex-1">
        <label class="block text-[10px] font-bold text-emerald-800 uppercase mb-1">Replaced By (Sub-Meter)</label>
        <select class="exc-replacement-id w-full rounded border-emerald-300 px-2 py-1.5 text-xs outline-none bg-white" data-selected="${repId}">
          <!-- Options populated by updateExceptionDropdowns() -->
        </select>
      </div>
      <button type="button" onclick="this.closest('.exc-row').remove(); triggerAutoSave();" class="mt-5 text-slate-400 hover:text-red-500">
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
      </button>
    `;"""

new_add_exception_row = """  function addExceptionRow(stolenNum="", repId="", dateStolen="", dateReplaced="") {
    const container = document.getElementById('exceptions-container');
    const row = document.createElement('div');
    row.className = "flex flex-col md:flex-row md:items-center space-y-3 md:space-y-0 md:space-x-4 bg-white p-3 rounded-lg border border-rose-200 shadow-sm exc-row";
    
    row.innerHTML = `
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
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
      </button>
    `;"""

content = content.replace(old_add_exception_row, new_add_exception_row)

# Update gatherExceptions
old_gather_exceptions = """  function gatherExceptions() {
    wizardData.exceptions = [];
    document.querySelectorAll('.exc-row').forEach(row => {
      const stolenNum = row.querySelector('.exc-stolen-num').value.trim();
      const repId = row.querySelector('.exc-replacement-id').value;
      if(stolenNum && repId) {
        wizardData.exceptions.push({ stolen_num: stolenNum, replacement_id: repId });
      }
    });
  }"""

new_gather_exceptions = """  function gatherExceptions() {
    wizardData.exceptions = [];
    document.querySelectorAll('.exc-row').forEach(row => {
      const stolenNum = row.querySelector('.exc-stolen-num').value.trim();
      const repId = row.querySelector('.exc-replacement-id').value;
      const dateStolen = row.querySelector('.exc-date-stolen').value;
      const dateReplaced = row.querySelector('.exc-date-replaced').value;
      
      if(stolenNum && repId) {
        wizardData.exceptions.push({ 
          stolen_num: stolenNum, 
          replacement_id: repId,
          date_stolen: dateStolen,
          date_replaced: dateReplaced
        });
      }
    });
  }"""

content = content.replace(old_gather_exceptions, new_gather_exceptions)

# Update loadDraft
old_load_draft_exc = "if(wizardData.exceptions) wizardData.exceptions.forEach(exc => addExceptionRow(exc.stolen_num, exc.replacement_id));"
new_load_draft_exc = "if(wizardData.exceptions) wizardData.exceptions.forEach(exc => addExceptionRow(exc.stolen_num, exc.replacement_id, exc.date_stolen || '', exc.date_replaced || ''));"
content = content.replace(old_load_draft_exc, new_load_draft_exc)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated setup_wizard UI and logic.")
