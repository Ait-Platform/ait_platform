import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace("filename='uploads/1ReadingState.png'", "filename='sace_slides/1ReadingState.png'")

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
