import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace flex-grow without min-h-0
text = text.replace('<div class="flex-grow overflow-hidden relative bg-slate-100">', '<div class="flex-grow overflow-hidden relative bg-slate-100 min-h-0">')
text = text.replace('<div class="w-full h-full hidden flex-col relative bg-slate-900" id="tab-f">', '<div class="w-full h-full hidden flex-col relative bg-slate-900 min-h-0" id="tab-f">')
text = text.replace('<div class="w-full h-full hidden flex-col bg-slate-100" id="tab-p">', '<div class="w-full h-full hidden flex-col bg-slate-100 min-h-0" id="tab-p">')
text = text.replace('<div class="flex-grow overflow-hidden relative">', '<div class="flex-grow overflow-hidden relative min-h-0">')
text = text.replace('<div class="flex-grow p-4 md:p-8 flex items-start justify-center overflow-y-auto">', '<div class="flex-grow p-4 md:p-8 flex items-start justify-center overflow-y-auto min-h-0">')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
