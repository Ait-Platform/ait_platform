import re

filepath = 'templates/program_uip/my_access.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

back_button_pattern = re.compile(
    r'<div class="mt-2">\s*<a href="\{\{ url_for\(\'uip_bp\.router_page\', org_slug=org\.slug, force=1\) \}\}" class="inline-block px-4 py-2 border border-slate-300 rounded-lg text-sm font-bold text-slate-600 hover:bg-slate-50 transition">\s*&larr; Role Selection\s*</a>\s*</div>'
)

content = back_button_pattern.sub('', content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Removed back button")
