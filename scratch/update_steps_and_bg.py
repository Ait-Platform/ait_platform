import re
file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Remove the unwanted P steps from simSteps
text = text.replace("        { slide: 6, view: 'p', appView: 3 },\n", "")
text = text.replace("        { slide: 8, view: 'p', appView: 4 },\n", "")
text = text.replace("        { slide: 12, view: 'p', appView: 5 },\n", "")

# 2. Change the F tab background from bg-slate-900 to bg-white
# Old: <div class="w-full h-full hidden flex-col relative bg-slate-900 min-h-0" id="tab-f">
text = text.replace('bg-slate-900', 'bg-white')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
