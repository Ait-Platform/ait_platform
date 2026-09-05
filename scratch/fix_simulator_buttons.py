import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the messy duplicated buttons section
pattern = r'<div class="flex space-x-2">.*?<!-- Dynamic Compliance Info'
replacement = """<div class="flex space-x-2">
            <button onclick="switchTab('a')" id="btn-tab-a" class="flex items-center px-4 py-2 bg-indigo-600 text-white font-bold rounded-t-lg transition hover:bg-indigo-500">
                <i class="fas fa-clipboard-check mr-2"></i> Auditor Guide (A)
            </button>
            <button onclick="switchTab('f')" id="btn-tab-f" class="flex items-center px-4 py-2 bg-slate-700 text-slate-300 font-bold rounded-t-lg transition hover:bg-slate-600 border-b-2 border-transparent">
                <div id="light-f" class="w-3 h-3 rounded-full bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.8)] mr-2"></div>
                Facilitator Board (F)
            </button>
            <button onclick="switchTab('p')" id="btn-tab-p" class="flex items-center px-4 py-2 bg-slate-700 text-slate-300 font-bold rounded-t-lg transition hover:bg-slate-600 border-b-2 border-transparent">
                <div id="light-p" class="w-3 h-3 rounded-full bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.8)] mr-2"></div>
                Participant Board (P)
            </button>
        </div>
        
        <!-- Dynamic Compliance Info"""

text = re.sub(pattern, replacement, text, flags=re.DOTALL)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
