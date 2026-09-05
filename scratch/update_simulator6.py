import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Flatten app-view-32
pattern = r'<div class="app-view hidden overflow-y-auto pb-20" id="app-view-32">.*?<div class="grid grid-cols-2 gap-2 text-xs font-medium text-slate-700">'
replacement = '''<div class="app-view hidden overflow-y-auto pb-20" id="app-view-32">
    <div class="p-4 mb-2">
      <h3 class="text-xl font-bold mb-1 text-slate-800"><i class="fas fa-tasks text-indigo-500 mr-2"></i>Classroom Application</h3>
      <p class="text-xs text-slate-500 border-b pb-4">Check all that are successfully included in the participant's practical/oral participation.</p>
    </div>
  
    <div class="bg-white px-4 pb-4">
      <div class="grid grid-cols-2 gap-2 text-xs font-medium text-slate-700">'''

text = re.sub(pattern, replacement, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
