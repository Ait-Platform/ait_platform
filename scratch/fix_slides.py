import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix the currentSlide offset issue so slide-lobby and slide-0 don't overlap
text = text.replace("let fId = currentSlide === 0 ? 'slide-lobby' : 'slide-' + currentSlide;", "let fId = currentSlide === -1 ? 'slide-lobby' : 'slide-' + currentSlide;")

# Update launchDemo to start at slide 0
text = text.replace("currentSlide = 0;", "currentSlide = 0;")

# Make sure starting slide in global is -1 (Lobby)
text = text.replace("let currentSlide = 0;", "let currentSlide = -1;")

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
