import re

filepath = 'templates/program_uip/dashboards/ratification_desk.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''<label class="flex justify-between items-end mb-2">
                        <span class="block text-xs font-bold text-slate-400 uppercase tracking-widest">Mandate Content</span>
                        <button type="button" onclick="document.querySelector('textarea[name=\\'description\\']').value = 'Please refer to the official attached mandate document for full details.'" class="text-[10px] bg-slate-700 hover:bg-slate-600 text-amber-400 px-2 py-1 rounded font-bold uppercase tracking-wider transition">
                            <i class="fas fa-file-pdf mr-1"></i> Use "Refer to Attachment"
                        </button>
                    </label>'''

# Replace the existing label
content = re.sub(
    r'<label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Mandate Content</label>',
    replacement,
    content
)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Added helper button")
