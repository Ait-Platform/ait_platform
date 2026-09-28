import re

filepath = 'templates/program_uip/dashboards/ratification_desk.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add Mandate Content textarea before Date/Location
date_location_pattern = r'<div class="grid grid-cols-2 gap-6">'
mandate_content = '''<div class="mb-6">
                  <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Mandate Content</label>
                  <textarea name="description" rows="5" class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-3 text-white focus:outline-none focus:border-amber-500 font-serif">{{ resolution.description }}</textarea>
                  <p class="text-[10px] text-slate-500 mt-1">Review or adjust the final recorded wording before saving.</p>
              </div>
              
              <div class="grid grid-cols-2 gap-6">'''
content = content.replace('<div class="grid grid-cols-2 gap-6">', mandate_content)

# 2. Replace Tally and Buttons with new simple button
tally_start = content.find('<div>\\n                  <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Live Meeting')
if tally_start == -1:
    # Let's search using a looser pattern
    match = re.search(r'<div>\s*<label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Live Meeting Vote Tally.*?</form>', content, re.DOTALL)
    if match:
        new_bottom = '''<div class="pt-4">
                  <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Upload Proof (PDF)</label>
                  <input type="file" name="mandate_file" accept=".pdf,.png,.jpg,.jpeg" class="block w-full text-sm text-slate-400 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-bold file:bg-amber-500/20 file:text-amber-500 hover:file:bg-amber-500/30 bg-slate-800 rounded-lg" />
                  <p class="text-xs text-slate-500 mt-2">Optional: Upload the official letter or AGM minutes to provide a permanent audit trail.</p>
              </div>
              
              <div class="pt-6 mt-4 border-t border-slate-800 flex justify-end">
                  <button type="submit" name="decision" value="ADOPTED" class="px-8 py-3 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg font-bold shadow transition text-lg">
                      <i class="fas fa-save mr-2"></i> Save Ratification Record
                  </button>
              </div>
          </form>'''
        content = content[:match.start()] + new_bottom + content[match.end():]

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("ratification desk updated")
