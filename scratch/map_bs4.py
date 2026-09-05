from bs4 import BeautifulSoup
import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = BeautifulSoup(html, 'html.parser')

# We will modify the DOM directly

# 1. Update Slide 0 to be 1Program.png
slide_0 = soup.find(id='slide-0')
slide_0.clear()
img_0 = soup.new_tag('img', alt="Slide 1: Program", src="{{ url_for('static', filename='sace_slides/1Program.png') }}")
img_0['class'] = "max-h-[60vh] max-w-full object-contain mx-auto"
slide_0.append(img_0)
slide_0['class'] = "slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" # make hidden initially
# Wait, slide-0 shouldn't be hidden if we launch, but our JS controls that by adding/removing 'flex' and 'hidden'.
# Actually, the python soup will mess up Jinja tags if not careful, but {{ url_for... }} is usually fine if we just assign it as a string and don't let bs4 escape it. Wait, bs4 escapes curly braces in attributes!
