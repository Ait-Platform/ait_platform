import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Extract everything BEFORE slide-0
match_before = re.search(r'^(.*?)<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl text-center" id="slide-0">', text, re.DOTALL)
before = match_before.group(1)

# 2. Extract everything AFTER slide-4 (the old root cause HTML tally, which is now slide-5)
# Wait, the old slide-4 was the root cause tally. 
# Slide 0: Crisis poll
# Slide 1: ReadingState
# Slide 2: Crisis tally
# Slide 3: 2Problem
# Slide 4: Root Cause tally
# I will replace 0, 1, 2, 3 with the new 0, 1, 2, 3, 4, and bump the old 4 to 5, etc.
# Actually, the easiest way is to split the text by id="slide-X" and rebuild the list of slides!

import bs4
soup = bs4.BeautifulSoup(text, 'html.parser')

f_tab = soup.find(id='tab-f')
slides_container = f_tab.find('div', class_='flex-grow relative overflow-hidden')

slides = slides_container.find_all('div', class_=re.compile('^slide-container'))

# slides[0] is lobby
# slides[1] is old slide-0 (Crisis Poll HTML)
# slides[2] is old slide-1 (1ReadingState)
# slides[3] is old slide-2 (Crisis Tally HTML)
# slides[4] is old slide-3 (2Problem)
# slides[5] is old slide-4 (Root Cause Tally HTML)
# ... up to slide-11

# We will create the new slide 0 to 4
new_slides_html = """
<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center" id="slide-0">
    <img alt="Slide 1: Program" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/1Program.png') }}"/>
</div>

<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-1">
    <img alt="Slide 2: Crisis" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/2Crisis.png') }}"/>
</div>

<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl" id="slide-2">
    <img alt="Slide 3: Survey" class="max-h-[40vh] max-w-full object-contain mx-auto mb-6 shadow-sm rounded" src="{{ url_for('static', filename='sace_slides/3Survey.png') }}"/>
    <h2 class="text-2xl font-bold text-indigo-600 mb-6"><i class="fas fa-poll mr-3"></i>Live Room Data Tally</h2>
    <div class="w-full max-w-2xl space-y-6">
        <div>
            <div class="flex justify-between text-sm font-bold text-slate-600 mb-1">
                <span>TRUE (Crisis Exists)</span>
                <span><span id="slide-2-true-pct">0</span>%</span>
            </div>
            <div class="w-full bg-slate-200 rounded-full h-4 overflow-hidden shadow-inner">
                <div class="bg-emerald-500 h-4 rounded-full transition-all duration-500" style="width: 0%"></div>
            </div>
        </div>
        <div>
            <div class="flex justify-between text-sm font-bold text-slate-600 mb-1">
                <span>FALSE (No Crisis)</span>
                <span><span id="slide-2-false-pct">0</span>%</span>
            </div>
            <div class="w-full bg-slate-200 rounded-full h-4 overflow-hidden shadow-inner">
                <div class="bg-rose-500 h-4 rounded-full transition-all duration-500" style="width: 0%"></div>
            </div>
        </div>
    </div>
</div>

<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-3">
    <img alt="Slide 4: Study" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/4Study.png') }}"/>
</div>

<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-4">
    <img alt="Slide 5: Problem" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/5Problem.png') }}"/>
</div>
"""

new_slides_soup = bs4.BeautifulSoup(new_slides_html, 'html.parser')

# Remove the old slides 1 through 4 (which correspond to slide-0 to slide-3)
slides[1].extract()
slides[2].extract()
slides[3].extract()
slides[4].extract()

# Insert the new ones right after lobby
current = slides[0]
for new_slide in new_slides_soup.find_all('div', recursive=False):
    current.insert_after(new_slide)
    current = new_slide

# Now we need to re-number the remaining slides (old slide-4 becomes slide-5, etc.)
# Find all slides again
all_slides = slides_container.find_all('div', class_=re.compile('^slide-container'), recursive=False)
# all_slides[0] is lobby
# all_slides[1] is slide-0
# all_slides[2] is slide-1
# all_slides[3] is slide-2
# all_slides[4] is slide-3
# all_slides[5] is slide-4
# all_slides[6] should be slide-5, etc.

for i in range(6, len(all_slides)):
    new_id = f"slide-{i-1}"
    all_slides[i]['id'] = new_id
    
# Now, dump to string. Note: BS4 might escape jinja brackets if we aren't careful, but since we parsed and modified safely, it should be ok. 
# Wait, let's check if bs4 mangled {{ url_for }}.
html_out = str(soup)
html_out = html_out.replace("%7B%7B%20", "{{ ").replace("%20%7D%7D", " }}") # fix URL encoding of jinja if any

with open('scratch/test_out.html', 'w', encoding='utf-8') as f:
    f.write(html_out)
