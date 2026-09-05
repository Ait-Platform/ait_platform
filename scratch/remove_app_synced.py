import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

pattern = r'<div class="bg-indigo-600 p-4 text-center text-white font-bold shadow-md z-10 flex justify-between items-center">\s*<span>AIT App</span>\s*<span class="text-xs bg-green-400 text-green-900 px-2 py-1 rounded-full"><i class="fas fa-link mr-1"></i>Synced</span>\s*</div>'
text = re.sub(pattern, '', text)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
