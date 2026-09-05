import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Removing the specific checkboxes
items_to_remove = [
    r'<label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Learner participation"\s*/><span>Learner participation</span></label>',
    r'<label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Participant guidance"\s*/><span>Participant guidance</span></label>',
    r'<label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Reading practice"\s*/><span>Reading practice</span></label>',
    r'<label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Assessment"\s*/><span>Assessment</span></label>'
]

for item in items_to_remove:
    text = re.sub(item, '', text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
