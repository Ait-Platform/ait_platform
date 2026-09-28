import re

filepath = 'templates/program_uip/dashboards/ratification_desk.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove the helper button and reset label
old_label = '''<label class="flex justify-between items-end mb-2">
                        <span class="block text-xs font-bold text-slate-400 uppercase tracking-widest">Mandate Content</span>
                        <button type="button" onclick="document.querySelector('textarea[name=\\'description\\']').value = 'Please refer to the official attached mandate document for full details.'" class="text-[10px] bg-slate-700 hover:bg-slate-600 text-amber-400 px-2 py-1 rounded font-bold uppercase tracking-wider transition">
                            <i class="fas fa-file-pdf mr-1"></i> Use "Refer to Attachment"
                        </button>
                    </label>'''
new_label = '<label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Mandate Content</label>'
content = content.replace(old_label, new_label)

# 2. Force the textarea content and make it readonly
textarea_pattern = re.compile(r'<textarea name="description"[^>]*>\{\{ resolution\.description \}\}</textarea>')
forced_textarea = '''<textarea name="description" rows="3" readonly class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-3 text-slate-400 focus:outline-none font-mono text-sm whitespace-pre-wrap">Please refer to the official attached mandate document for full details.</textarea>'''
content = textarea_pattern.sub(forced_textarea, content)

# 3. Make the file upload required
file_input_pattern = re.compile(r'<input type="file" name="mandate_file" accept="\.pdf,\.png,\.jpg,\.jpeg" class="([^"]+)" />')
forced_file_input = r'<input type="file" name="mandate_file" accept=".pdf,.png,.jpg,.jpeg" required class="\1" />'
content = file_input_pattern.sub(forced_file_input, content)

# 4. Remove the helper text under the textarea and update helper text under file upload
content = content.replace('<p class="text-[10px] text-slate-500 mt-2">Review or adjust the final recorded wording before saving.</p>', '')
content = content.replace('Optional: Upload the official letter or AGM minutes to provide a permanent audit trail.', 'Required: Upload the official signed PDF to provide a permanent audit trail.')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("ratification desk forced pdf updated")
