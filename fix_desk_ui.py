import re

filepath = 'templates/program_uip/dashboards/mandate_recording_desk.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix "Mandate Content" textarea -> static div
old_textarea = '''<div class="mb-6">
                    <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Mandate Content</label>
                    <textarea name="description" rows="3" readonly class="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-3 text-slate-500 focus:outline-none font-mono text-sm whitespace-pre-wrap cursor-not-allowed">Please refer to the official attached mandate document for full details.</textarea>
                </div>'''

new_static = '''<div class="mb-6">
                    <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Mandate Content</label>
                    <div class="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-3 text-slate-800 font-medium text-sm">
                        Please refer to the official attached mandate document for full details.
                        <input type="hidden" name="description" value="Please refer to the official attached mandate document for full details.">
                    </div>
                </div>'''

content = content.replace(old_textarea, new_static)

# 2. Darken select text (already text-slate-800, maybe make it text-slate-900 font-bold)
content = content.replace('text-slate-800 font-bold', 'text-slate-900 font-bold')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Mandate UI fixed")
