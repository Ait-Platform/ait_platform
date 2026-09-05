import re

# 1. Update sace_catalog.html
file_path = 'templates/program_sace/sace_catalog.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()
text = text.replace('SACE Activities Catalog', 'Sace Activities for endorsement')
with open(file_path, 'w', encoding='utf-8') as f: f.write(text)

# 2. Update reading_hub.html
file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()
text = text.replace('SACE Endorsement: LITRE Reading', 'LITRE Reading Activity')
text = text.replace('SACE Endorsement Simulator (SES)', 'Reading Activity Simulator')
text = text.replace('1. View Linear Presentation (PPP)', '1. View Linear Presentation')
text = text.replace('Start here. Review the actual content of the course linearly.', 'Start here. Review a power point presentation of the course linearly.')

# Remove the Workshop Completion & Certification block
block_regex = r'<div class="mt-8 pt-8 border-t border-slate-700 relative z-10 text-center">\s*<h3 class="text-xl font-bold text-white mb-4">Workshop Completion & Certification</h3>.*?</div>\s*</div>\s*<div class="bg-slate-800 p-4 border-t border-slate-700 text-center">'
text = re.sub(block_regex, '</div>\n          <div class="bg-slate-800 p-4 border-t border-slate-700 text-center">', text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)

print("Updated catalog and reading hub.")
