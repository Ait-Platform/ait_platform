import re

file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

pattern_rg = r'\s*<!-- Reviewer Guide -->\s*<div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">\s*<div class="flex items-center w-1/3">\s*<i class="fas fa-book text-indigo-400 mr-3 text-xl"></i>\s*<span class="font-bold text-slate-700">Reviewer Guide</span>\s*</div>\s*<div class="w-1/3 text-center">\s*<a href="\{\{ url_for\(\'sace_bp\.secure_view\', doc_type=\'reviewer_guide\'\) \}\}" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition">View Document</a>\s*</div>\s*<div class="w-1/3 flex justify-end">\s*\{% if progress\.reviewer_guide %\}<i class="fas fa-check-circle text-green-500 text-2xl"></i>\{% else %\}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>\{% endif %\}\s*</div>\s*</div>'

text = re.sub(pattern_rg, '', text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
