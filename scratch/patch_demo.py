import re

filenames = [
    "1Program.png", "2Crisis.png", "3Survey.png", "4Study.png", "5The Problem.png", 
    "6Root Cause.png", "7Litre.png", "8Why Litre.png", "9What is Litre.png",
    "10Program.png", "11DrawPalm.png", "12Home.png", "13English Family.png", "14Vowels.png", 
    "15BlendingMachine.png", "16Program.png", "17ta.png", "18ma.png", "19tomato.png", 
    "20pa.png", "21Catch Up.png", "22ClassActivity.png", "23Assessment.png", "24Reflection.png", 
    "25Thank.png"
]

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Rename to Demo
text = text.replace('SACE Endorsement Simulator', 'SACE Interactive Demo')

# 2. Add caveat INSIDE tab-a
caveat_html = '''
<!-- Evaluator Caveat -->
<div class="bg-amber-50 border-l-4 border-amber-500 p-4 mb-6 shadow-sm rounded-r">
    <div class="flex">
        <div class="flex-shrink-0">
            <i class="fas fa-exclamation-triangle text-amber-500"></i>
        </div>
        <div class="ml-3">
            <h3 class="text-sm font-bold text-amber-800">Evaluator Note: Interactive Demonstration</h3>
            <div class="mt-1 text-sm text-amber-700">
                <p>This environment is a <strong>structural demonstration</strong> of how the AIT platform synchronizes facilitator and participant devices. For endorsement purposes, you may also view the <strong>Linear Presentation (PPP)</strong> from the SACE Hub.</p>
            </div>
        </div>
    </div>
</div>
'''

tab_a_start = '<div class="w-full h-full flex flex-col bg-white overflow-y-auto" id="tab-a">\n            <div class="p-8 md:p-12 max-w-3xl mx-auto w-full">'
if tab_a_start in text:
    text = text.replace(tab_a_start, tab_a_start + '\n' + caveat_html)
else:
    print("Could not find tab-a start")

# 3. Replace slides
# Find where slide-0 starts and the last placeholder ends
start_marker = '<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl text-center z-20" id="slide-lobby">'
end_marker = '<!-- P Board Controller (Hidden on F) -->'
# Actually, let's just replace everything from slide-0 to slide-15
import re
slides_pattern = re.compile(r'<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center" id="slide-0">.*?(?=<!-- P Board Controller \(Hidden on F\) -->)', re.DOTALL)

new_slides_html = ""
for i, filename in enumerate(filenames):
    display = 'flex' if i == 0 else 'hidden'
    new_slides_html += f'''
            <div class="slide-container absolute inset-0 {display} flex-col overflow-y-auto items-center justify-center" id="slide-{i}">
                <img alt="Slide {i+1}" class="w-full h-full p-2 object-contain mx-auto" src="{{{{ url_for('static', filename='sace_slides/{filename}') }}}}"/>
            </div>'''

text = slides_pattern.sub(new_slides_html + '\n            </div>\n            ', text)

# 4. Update total slides in JS
text = re.sub(r'const totalSlides = \d+;', f'const totalSlides = {len(filenames)};', text)
text = re.sub(r'Step <span id="f-counter">0</span> of \d+', f'Step <span id="f-counter">0</span> of {len(filenames)}', text)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
