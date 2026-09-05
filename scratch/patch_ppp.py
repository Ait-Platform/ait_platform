import re

filenames = [
    "1Program.png", "2Crisis.png", "3Survey.png", "4Study.png", "5The Problem.png", 
    "6Root Cause.png", "7Litre.png", "8Why Litre.png", "9What is Litre.png",
    "10Program.png", "11DrawPalm.png", "12Home.png", "13English Family.png", "14Vowels.png", 
    "15BlendingMachine.png", "16Program.png", "17ta.png", "18ma.png", "19tomato.png", 
    "20pa.png", "21Catch Up.png", "22ClassActivity.png", "23Assessment.png", "24Reflection.png", 
    "25Thank.png"
]

with open('templates/program_sace/presentation_ppp.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the slides array
new_slides_js = "const slides = [\n"
for i, filename in enumerate(filenames):
    # Only some slides have audio. Let's just use the old ones. The user hasn't specified audio for 10-25 yet, so we'll just put null for now.
    audio = 'null'
    if filename == "1Program.png":
        audio = '"{{ url_for(\'static\', filename=\'sace_slides/1_audio.wav\') }}"'
    elif filename == "2Crisis.png":
        audio = '"{{ url_for(\'static\', filename=\'sace_slides/2_audio.wav\') }}"'
    elif filename == "4Study.png":
        audio = '"{{ url_for(\'static\', filename=\'sace_slides/4_audio.wav\') }}"'
        
    new_slides_js += f'        {{ img: "{{{{ url_for(\'static\', filename=\'sace_slides/{filename}\') }}}}", audio: {audio} }},\n'
new_slides_js += "    ];"

# We need to find the old slides array and replace it
slides_pattern = re.compile(r'const slides = \[.*?\];', re.DOTALL)
text = slides_pattern.sub(new_slides_js, text)

# Ensure the counter total starts correct
text = re.sub(r'<span id="counter-total">\d+</span>', f'<span id="counter-total">{len(filenames)}</span>', text)

with open('templates/program_sace/presentation_ppp.html', 'w', encoding='utf-8') as f:
    f.write(text)
