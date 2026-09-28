import re

filepath = 'templates/program_uip/dashboards/ratification_desk.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update form to allow file upload
content = content.replace('class="space-y-6">', 'class="space-y-6" enctype="multipart/form-data">')

# 2. Make live_yea, live_nay, live_abstain optional (remove 'required')
content = content.replace('name="live_yea" min="0" required', 'name="live_yea" min="0"')
content = content.replace('name="live_nay" min="0" required', 'name="live_nay" min="0"')
content = content.replace('name="live_abstain" min="0" required', 'name="live_abstain" min="0"')

# 3. Add a note next to "Live Meeting Vote Tally"
tally_label = '<label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Live Meeting Vote Tally</label>'
new_tally_label = '<label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Live Meeting Vote Tally <span class="normal-case text-slate-500 text-[10px] ml-2">(Optional for Foundational Mandates)</span></label>'
content = content.replace(tally_label, new_tally_label)

# 4. Add the File Upload for Mandate Proof before the submission buttons
buttons_section = '''              <div class="mt-8 flex justify-end gap-3 pt-6 border-t border-slate-800">'''

file_upload_section = '''              <div class="pt-4">
                  <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Upload Proof (PDF)</label>
                  <input type="file" name="mandate_file" accept=".pdf,.png,.jpg,.jpeg" class="block w-full text-sm text-slate-400 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-bold file:bg-amber-500/20 file:text-amber-500 hover:file:bg-amber-500/30 bg-slate-800 rounded-lg" />
                  <p class="text-xs text-slate-500 mt-2">Optional: Upload the official letter or AGM minutes to provide a permanent audit trail.</p>
              </div>
              
              <div class="mt-8 flex justify-end gap-3 pt-6 border-t border-slate-800">'''

content = content.replace(buttons_section, file_upload_section)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("ratification_desk patched")
