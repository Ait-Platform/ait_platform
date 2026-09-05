import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Make sure total slides is 18
content = re.sub(r'const totalSlides = \d+;', 'const totalSlides = 18;', content)
content = re.sub(r'<span id="slide-counter" class="font-bold text-slate-600">0 / \d+</span>', '<span id="slide-counter" class="font-bold text-slate-600">0 / 18</span>', content)

slides_html = """
                    <!-- Slide 14 (pa) -->
                    <div id="slide-14" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/13pa.png') }}" class="max-h-full max-w-full object-contain" alt="pa">
                    </div>
                    
                    <!-- Slide 15 (Class Activity) -->
                    <div id="slide-15" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/15ClassActivity.png') }}" class="max-h-full max-w-full object-contain" alt="Class Activity">
                    </div>

                    <!-- Slide 16 (Assessment) -->
                    <div id="slide-16" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative border-4 border-indigo-500">
                        <img src="{{ url_for('static', filename='sace_slides/16 Assessment.png') }}" class="max-h-full max-w-full object-contain" alt="Assessment">
                        <div class="absolute bottom-6 bg-indigo-900/90 p-4 rounded-lg text-center border border-indigo-500 shadow-2xl backdrop-blur-sm z-10 text-white">
                            <h3 class="font-bold text-yellow-300 text-sm mb-1"><i class="fas fa-award mr-2"></i>Final Assessment Active</h3>
                            <p class="text-xs">Participants are completing the final assessment on their devices.</p>
                        </div>
                    </div>
                    
                    <!-- Slide 17 (Reflection) -->
                    <div id="slide-17" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/17Reflection.png') }}" class="max-h-full max-w-full object-contain" alt="Reflection">
                    </div>
                    
                    <!-- Slide 18 (Thank You) -->
                    <div id="slide-18" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/18Thank.png') }}" class="max-h-full max-w-full object-contain" alt="Thank You">
                    </div>
"""

content = content.replace("<!-- Slide 14 (pa) -->", "")
content = content.replace("<!-- Slide 15 (Class Activity) -->", "")
content = content.replace("<!-- Slide 16 (Assessment) -->", "")

# Insert right after slide-13
pattern = re.compile(r'<!-- Slide 13 \(tomato\) -->.*?</div>', re.DOTALL)
match = pattern.search(content)
if match:
    insert_pos = match.end()
    content = content[:insert_pos] + '\n' + slides_html + content[insert_pos:]

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
