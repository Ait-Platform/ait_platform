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

# 3. Replace slides
# Find where slide-0 starts and the last placeholder ends
slides_pattern = re.compile(r'<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center" id="slide-0">.*?(?=<!-- P Board Controller \(Hidden on F\) -->)', re.DOTALL)

new_slides_html = ""
for i, filename in enumerate(filenames):
    display = 'flex' if i == 0 else 'hidden'
    new_slides_html += f'''
            <div class="slide-container absolute inset-0 {display} flex-col overflow-y-auto items-center justify-center" id="slide-{i}">
                <img alt="Slide {i+1}" class="w-full h-full p-2 object-contain mx-auto" src="{{{{ url_for('static', filename='sace_slides/{filename}') }}}}"/>
            </div>'''

text = slides_pattern.sub(new_slides_html + '\n            </div>\n            </div>\n            ', text)

# 4. Update total slides in JS
text = re.sub(r'const totalSlides = \d+;', f'const totalSlides = {len(filenames)};', text)
text = re.sub(r'Step <span id="f-counter">0</span> of \d+', f'Step <span id="f-counter">0</span> of {len(filenames)}', text)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
