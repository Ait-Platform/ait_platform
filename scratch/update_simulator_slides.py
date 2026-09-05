import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace Tab A Header
text = text.replace('Guide A', 'Sace Auditor')
text = text.replace('TAB A: GUIDE', 'TAB A: SACE AUDITOR')

# Empty the waiting screen text
old_waiting = '''<h3 class="text-2xl font-bold text-slate-800 mb-2">You're Checked In!</h3>
<p class="text-slate-600 mb-6">Please look at the projector. The facilitator will begin the session shortly.</p>'''
new_waiting = '''<h3 class="text-2xl font-bold text-slate-800 mb-2">You're Checked In!</h3>
<p class="text-slate-600 mb-6">Waiting for activity to begin...</p>'''
text = text.replace(old_waiting, new_waiting)

# Replace the slides HTML
slides_html = ""
files = [
    "1Program.png", "2Crisis.png", "3Survey.png", "4Study.png", "5The Problem.png",
    "6Root Cause.png", "7Litre.png", "8Why Litre.png", "9What is Litre.png", "10Program.png",
    "11DrawPalm.png", "12Home.png", "13English Family.png", "14Vowels.png", "15a Tactile.png",
    "15BlendingMachine.png", "16Program.png", "17ta.png", "18ma.png", "19aSign Language.png",
    "19bSign Language.png", "19tomato.png", "20pa.png", "21Catch Up.png", "22ClassActivity.png",
    "23Assessment.png", "24Reflection.png", "25Thank.png"
]

for i, filename in enumerate(files):
    display_class = 'flex' if i == 0 else 'hidden'
    slides_html += f'''
            <div class="slide-container absolute inset-0 {display_class} flex-col overflow-y-auto items-center justify-center" id="slide-{i}">
                <img alt="Slide {i+1}" class="w-full h-full p-2 object-contain mx-auto" src="{{{{ url_for('static', filename='sace_slides/{filename}') }}}}"/>
            </div>'''

# Replace from id="slide-0" to id="slide-24"
pattern = r'<div class="slide-container absolute inset-0 flex flex-col.*?id="slide-0".*?id="slide-24".*?</div>'
text = re.sub(pattern, slides_html.strip(), text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

