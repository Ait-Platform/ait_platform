import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Replace the slide HTML containers
slides_html = ""
for i in range(30):
    display_class = 'flex' if i == 0 else 'hidden'
    slides_html += f'''
            <div class="slide-container absolute inset-0 {display_class} flex-col overflow-y-auto items-center justify-center" id="slide-{i}">
                <img alt="Slide {i+1}" class="w-full h-full p-2 object-contain mx-auto" src="{{{{ url_for('static', filename='sace_slides/{i+1}.png') }}}}"/>
            </div>'''

# We need to find the block of slide containers to replace.
pattern = r'<div class="slide-container absolute inset-0 flex flex-col.*?id="slide-0".*?id="slide-27".*?</div>'
text = re.sub(pattern, slides_html.strip(), text, flags=re.DOTALL)

# 2. Update the counter text in HTML
text = text.replace('Step <span id="f-counter-global">0</span> of 28', 'Step <span id="f-counter-global">0</span> of 30')

# 3. Rebuild the simSteps JS array
# 30 'f' slides, and 1 'p' slide for the Assessment (appView: 10) at the end.
new_steps_js = "const simSteps = [\n"
for i in range(30):
    new_steps_js += f"        {{ slide: {i}, view: 'f' }},\n"
# Add the final assessment interactivity to the last slide
new_steps_js += "        { slide: 29, view: 'p', appView: 10 }\n    ];"

steps_pattern = r'const simSteps = \[.*?\];'
text = re.sub(steps_pattern, new_steps_js, text, flags=re.DOTALL)

# 4. Update JS counter
text = text.replace('displaySlide + " of 28"', 'displaySlide + " of 30"')
text = text.replace('displaySlide;', 'displaySlide;') # Just making sure it sets displaySlide only, wait, earlier I fixed this to just displaySlide;.
text = text.replace("document.getElementById('f-counter-global').innerText = displaySlide;", "document.getElementById('f-counter-global').innerText = displaySlide;")

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

