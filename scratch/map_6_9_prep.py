import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update 5Problem to 5The Problem
text = text.replace("filename='sace_slides/5Problem.png'", "filename='sace_slides/5The Problem.png'")

# 2. Insert 6Root Cause after 5The Problem
# Find the end of slide-4
old_slide_4 = """<img alt="Slide 5: Problem" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/5The Problem.png') }}"/>
                    </div>"""
new_slide_4 = """<img alt="Slide 5: Problem" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/5The Problem.png') }}"/>
                    </div>
                    
                    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-5">
                        <img alt="Slide 6: Root Cause" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/6Root Cause.png') }}"/>
                    </div>"""
text = text.replace(old_slide_4, new_slide_4)

# 3. Replace old Slide 8 (3Intro.png) and inject 7Litre, 8Why Litre, 9What is Litre
old_slide_8 = """<div class="slide-container absolute inset-0 hidden overflow-y-auto items-center justify-center" id="slide-8">
                        <img alt="Slide 3: Introduction" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/3Intro.png') }}"/>
                    </div>"""
new_slides_9_11 = """<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-9">
                        <img alt="Slide 7: Litre" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/7Litre.png') }}"/>
                    </div>
                    
                    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-10">
                        <img alt="Slide 8: Why Litre" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/8Why Litre.png') }}"/>
                    </div>
                    
                    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-11">
                        <img alt="Slide 9: What is Litre" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/9What is Litre.png') }}"/>
                    </div>"""
text = text.replace(old_slide_8, new_slides_9_11)

# Now wait! The HTML id attributes for all slides after slide-4 must be incremented by 1!
# AND the slides after slide-8 must be incremented by 3!
# This is tricky with simple regex. Let's use bs4 again!
