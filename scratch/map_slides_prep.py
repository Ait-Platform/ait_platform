import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace slide-0 (Was HTML Welcome Activity, now 1Program.png)
old_slide_0 = re.search(r'<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl text-center" id="slide-0">.*?</div>\s*</div>\s*</div>\s*<div class="slide-container absolute inset-0 hidden', text, re.DOTALL).group(0)

new_slide_0 = """<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center" id="slide-0">
    <img alt="Slide 1: Program" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/1Program.png') }}"/>
</div>
<div class="slide-container absolute inset-0 hidden"""
text = text.replace(old_slide_0, new_slide_0)

# Replace slide-1 (Was 1ReadingState.png, now 2Crisis.png)
old_slide_1 = re.search(r'<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-1">.*?</div>\s*<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl" id="slide-2">', text, re.DOTALL).group(0)

new_slide_1 = """<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-1">
    <img alt="Slide 2: Crisis" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/2Crisis.png') }}"/>
</div>
<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl" id="slide-2">"""
text = text.replace(old_slide_1, new_slide_1)

# Modify slide-2 (Was just HTML Tally, now 3Survey.png + Tally)
old_slide_2_top = '<h2 class="text-3xl font-bold text-indigo-600 mb-8"><i class="fas fa-poll mr-3"></i>Live Room Data: Pre-Test</h2>\n                        <p class="text-xl text-indigo-700 mb-8 font-semibold">Q: Is there a crisis in reading in SA?</p>'
new_slide_2_top = """<img alt="Slide 3: Survey" class="max-h-[40vh] max-w-full object-contain mx-auto mb-4" src="{{ url_for('static', filename='sace_slides/3Survey.png') }}"/>
                        <h2 class="text-2xl font-bold text-indigo-600 mb-4"><i class="fas fa-poll mr-3"></i>Live Room Data Tally</h2>"""
text = text.replace(old_slide_2_top, new_slide_2_top)

# Replace slide-3 (Was 2Problem.png, now 4Study.png)
text = text.replace("filename='sace_slides/2Problem.png'", "filename='sace_slides/4Study.png'")
text = text.replace('alt="Slide 2: The Problem"', 'alt="Slide 4: Study"')

# Insert 5Problem.png logic into slide-4 (Was Root Cause Tally, now it needs to be 5Problem.png)
# Wait, if slide-4 is 5Problem.png, where does the Root Cause Tally go? It bumps to slide-5!
# Let's completely rewrite the P-board index mapping and Slide 0-5 in JS.
