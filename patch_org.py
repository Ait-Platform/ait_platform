with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

import re
old_div = r'<div class="text-\[10px\] font-bold text-indigo-400 uppercase tracking-widest mb-2">\{\{ seat.qualifier \}\}</div>'
new_div = r'''<div class="flex justify-between items-center mb-2">
                        <div class="text-[10px] font-bold text-indigo-400 uppercase tracking-widest">{{ seat.qualifier }}</div>
                        <div class="text-[9px] font-bold text-white bg-slate-800 px-1.5 py-0.5 rounded-full" title="Platform Permission">
                            <i class="fas fa-key mr-1"></i>{{ seat.duty|default('committee_member')|replace('_', ' ')|title }}
                        </div>
                    </div>'''

text = re.sub(old_div, new_div, text)
with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated seat qualifier with duty tag")
