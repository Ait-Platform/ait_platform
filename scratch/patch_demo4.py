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

start_str = '<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center" id="slide-0">'
end_str = '<h2 class="text-3xl font-bold text-emerald-400 mb-2 uppercase tracking-wider">Slide 16: Assessment</h2>\n          </div>'

idx1 = text.find(start_str)
idx2 = text.find(end_str)

if idx1 != -1 and idx2 != -1:
    new_slides_html = ""
    for i, filename in enumerate(filenames):
        display = 'flex' if i == 0 else 'hidden'
        new_slides_html += f'''
          <div class="slide-container absolute inset-0 {display} flex-col overflow-y-auto items-center justify-center" id="slide-{i}">
              <img alt="Slide {i+1}" class="w-full h-full p-2 object-contain mx-auto" src="{{{{ url_for('static', filename='sace_slides/{filename}') }}}}"/>
          </div>'''
    
    text = text[:idx1] + new_slides_html.strip() + "\n" + text[idx2 + len(end_str):]
else:
    print("Could not find start or end strings")

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
