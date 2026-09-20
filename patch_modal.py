with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

import re
old_form = r'<form method="POST" action="\{\{ url_for\(\'uip_bp.secretary_organogram\', org_slug=org.slug\) \}\}" class="p-6\s+space-y-4">'
new_form = r'<form method="POST" action="{{ url_for(\'uip_bp.secretary_organogram\', org_slug=org.slug) }}" class="p-6 space-y-4" enctype="multipart/form-data">'

text = re.sub(old_form, new_form, text)

old_input = r'''<div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Photo URL</label>
                  <input type="url" name="photo_url" required placeholder="https://example.com/photo.jpg" 
class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                  <p class="text-\[10px\] text-slate-500 mt-1">For this prototype, please provide a direct image URL 
\(e.g. from LinkedIn or Imgur\).</p>
              </div>'''

new_input = r'''<div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Select Photo</label>
                  <input type="file" name="photo_file" accept="image/*" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                  <p class="text-[10px] text-slate-500 mt-1">Upload a JPG or PNG. The file will be securely stored in Cloudflare R2.</p>
              </div>'''

text = re.sub(old_input.replace('\n', r'\s*'), new_input, text, flags=re.MULTILINE|re.IGNORECASE)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated Organogram photo modal for file upload")
