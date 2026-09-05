import re

with open('templates/program_sace/sace_catalog.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_card = """                    <h2 class="text-xl font-bold text-slate-800 mb-2 group-hover:text-indigo-600">{{ activity.name }}</h2>
                    <p class="text-sm text-slate-500 flex-grow">{{ activity.desc }}</p>
                    <div class="mt-4 pt-4 border-t border-slate-100 text-indigo-600 font-bold text-sm flex items-center justify-end group-hover:text-indigo-800">"""

new_card = """                    <h2 class="text-xl font-bold text-slate-800 mb-2 group-hover:text-indigo-600">{{ activity.name }}</h2>
                    <p class="text-sm text-slate-500 flex-grow mb-4">{{ activity.desc }}</p>
                    
                    <!-- Schedule Info -->
                    <div class="bg-slate-50 p-3 rounded-lg border border-slate-100 mb-4">
                        <div class="text-xs font-bold text-slate-500 uppercase tracking-wide mb-1">Next Live Session</div>
                        <div class="text-sm font-semibold text-slate-800 flex items-center"><i class="far fa-calendar-alt w-5 text-indigo-500"></i> Oct 12, 2026 @ 09:00 AM</div>
                        <div class="text-sm text-slate-600 flex items-center mt-1"><i class="fas fa-map-marker-alt w-5 text-indigo-500"></i> Sandton Convention Centre, JHB</div>
                    </div>

                    <div class="mt-auto pt-4 border-t border-slate-100 text-indigo-600 font-bold text-sm flex items-center justify-end group-hover:text-indigo-800">"""

content = content.replace(old_card, new_card)

with open('templates/program_sace/sace_catalog.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated catalog UI with schedule info")
