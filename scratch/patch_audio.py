import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add audio tags back
slide_0_old = '<img alt="Slide 1: Program" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for(\'static\', filename=\'sace_slides/1Program.png\') }}"/>'
slide_0_new = slide_0_old + '\n              <audio class="hidden"><source src="{{ url_for(\'static\', filename=\'sace_slides/1_audio.wav\') }}" type="audio/wav"/></audio>'
text = text.replace(slide_0_old, slide_0_new)

slide_1_old = '<img alt="Slide 2: Crisis" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for(\'static\', filename=\'sace_slides/2Crisis.png\') }}"/>'
slide_1_new = slide_1_old + '\n              <audio class="hidden"><source src="{{ url_for(\'static\', filename=\'sace_slides/2_audio.wav\') }}" type="audio/wav"/></audio>'
text = text.replace(slide_1_old, slide_1_new)

slide_3_old = '<img alt="Slide 4: Study" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for(\'static\', filename=\'sace_slides/4Study.png\') }}"/>'
slide_3_new = slide_3_old + '\n              <audio class="hidden"><source src="{{ url_for(\'static\', filename=\'sace_slides/4_audio.wav\') }}" type="audio/wav"/></audio>'
text = text.replace(slide_3_old, slide_3_new)

# Fix F-board footer buttons
footer_search = '<button class="hidden px-6 py-4 bg-purple-600 hover:bg-purple-500 text-white font-bold text-lg rounded-xl transition \nshadow-[0_0_15px_rgba(147,51,234,0.5)]" id="global-audio-btn" onclick="playCurrentSlideAudio()">'
if footer_search not in text:
    footer_search = '<button class="hidden px-6 py-4 bg-purple-600 hover:bg-purple-500 text-white font-bold text-lg rounded-xl transition shadow-[0_0_15px_rgba(147,51,234,0.5)]" id="global-audio-btn" onclick="playCurrentSlideAudio()">'

footer_replace = '''<button class="px-6 py-4 bg-slate-300 hover:bg-slate-400 text-slate-700 font-bold rounded-xl transition" onclick="prevSlide()">
                                  <i class="fas fa-chevron-left mr-2"></i> Prev Slide
                              </button>
                              <button class="px-6 py-4 bg-slate-300 hover:bg-slate-400 text-slate-700 font-bold rounded-xl transition" onclick="nextSlide()">
                                  Next Slide <i class="fas fa-chevron-right ml-2"></i>
                              </button>
                              ''' + footer_search

text = text.replace(footer_search, footer_replace)

# Add JS functions
js_insert = '''
    function nextSlide() {
        if (currentSlide < totalSlides) {
            currentSlide++;
            updateSlides();
        } else {
            alert("Simulation Complete! Endorsement demonstration finished.");
            showTab('a');
        }
    }

    function prevSlide() {
        if (currentSlide > 0) {
            currentSlide--;
            updateSlides();
        }
    }
'''

text = text.replace('function finishActivityAndNext() {', js_insert + '\n    function finishActivityAndNext() {')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
