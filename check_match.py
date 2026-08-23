import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

vehicle_match = re.search(r'          <div class="bg-white border border-slate-200 rounded-xl p-5 shadow-sm relative group">\s*<div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">\s*<h3 class="text-sm font-bold text-slate-500 uppercase tracking-wider">Vehicle Details</h3>.*?</div>', content, re.DOTALL)
if vehicle_match:
    print("MATCHED")
else:
    print("NO MATCH")
