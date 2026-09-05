import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add 1_audio.wav to 1Program
audio_1 = """<img alt="Slide 1: Program" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/1Program.png') }}"/>
        <audio class="hidden"><source src="{{ url_for('static', filename='sace_slides/1_audio.wav') }}" type="audio/wav"></audio>"""
text = text.replace("""<img alt="Slide 1: Program" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/1Program.png') }}"/>""", audio_1)

# Add 2_audio.wav to 2Crisis
audio_2 = """<img alt="Slide 2: Crisis" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/2Crisis.png') }}"/>
        <audio class="hidden"><source src="{{ url_for('static', filename='sace_slides/2_audio.wav') }}" type="audio/wav"></audio>"""
text = text.replace("""<img alt="Slide 2: Crisis" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/2Crisis.png') }}"/>""", audio_2)

# Add 4_audio.wav to 4Study
audio_4 = """<img alt="Slide 4: Study" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/4Study.png') }}"/>
        <audio class="hidden"><source src="{{ url_for('static', filename='sace_slides/4_audio.wav') }}" type="audio/wav"></audio>"""
text = text.replace("""<img alt="Slide 4: Study" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/4Study.png') }}"/>""", audio_4)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
