import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update Tab A header
old_tab_a_header = '<h2 class="text-3xl font-extrabold text-indigo-900 mb-6">SACE Auditor Guide</h2>'
new_tab_a_header = '''<div class="flex justify-between items-center mb-6">
    <h2 class="text-3xl font-extrabold text-indigo-900">SACE Auditor Program</h2>
    <button class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-black text-lg rounded-xl shadow-lg transition flex items-center" onclick="launchDemo()">
        Launch Full SACE Program <i class="fas fa-arrow-right ml-3"></i>
    </button>
</div>'''

if old_tab_a_header in text:
    text = text.replace(old_tab_a_header, new_tab_a_header)

# 2. Remove old launch button at the bottom of tab-a
old_btn_pattern = r'<button class="px-8 py-4 bg-indigo-600 hover:bg-indigo-700 text-white font-black text-xl rounded-xl shadow-lg transition flex items-center" onclick="launchDemo\(\)">.*?Launch Full SACE Program <i class="fas fa-arrow-right ml-3"></i>\s*</button>'
text = re.sub(old_btn_pattern, '', text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
