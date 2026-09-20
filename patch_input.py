with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

import re
old_input = r"""<div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Photo URL</label>
                  <input type="url" name="photo_url" required placeholder="https://example.com/photo.jpg" 
class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                  <p class="text-[10px] text-slate-500 mt-1">For this prototype, please provide a direct image URL 
(e.g. from LinkedIn or Imgur).</p>
              </div>"""

new_input = """<div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Select Photo</label>
                  <input type="file" name="photo_file" accept="image/*" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                  <p class="text-[10px] text-slate-500 mt-1">Upload a JPG or PNG. The file will be securely stored in Cloudflare R2.</p>
              </div>"""

# Find the div using simple string search of the label
start_idx = text.find('<label class="block text-xs font-bold text-slate-700 mb-1">Photo URL</label>')
if start_idx != -1:
    div_start = text.rfind('<div>', 0, start_idx)
    div_end = text.find('</div>', start_idx) + 6
    text = text[:div_start] + new_input + text[div_end:]
    
with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated photo input")
