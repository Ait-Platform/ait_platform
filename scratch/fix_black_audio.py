import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix 1: Black slides (Change bg-slate-900 to bg-slate-100 on tab-f)
text = text.replace('<div id="tab-f" class="w-full h-full hidden flex-col relative bg-slate-900">', 
                    '<div id="tab-f" class="w-full h-full hidden flex-col relative bg-slate-100">')

# Fix 2: Inject Audio Button into F Controller
old_f_controller_buttons = """<button onclick="pushToParticipant()" class="px-8 py-4 bg-emerald-600 hover:bg-emerald-500 text-white font-black text-xl rounded-xl transition shadow-[0_0_20px_rgba(16,185,129,0.5)]">
                        Push to Teacher's Device <i class="fas fa-arrow-right ml-3"></i>
                    </button>"""

new_f_controller_buttons = """<div class="flex space-x-4 items-center">
                        <button id="global-audio-btn" onclick="playCurrentSlideAudio()" class="hidden px-6 py-4 bg-purple-600 hover:bg-purple-500 text-white font-bold text-lg rounded-xl transition shadow-[0_0_15px_rgba(147,51,234,0.5)]">
                            <i class="fas fa-play mr-2"></i> Play Audio
                        </button>
                        <button onclick="pushToParticipant()" class="px-8 py-4 bg-emerald-600 hover:bg-emerald-500 text-white font-black text-xl rounded-xl transition shadow-[0_0_20px_rgba(16,185,129,0.5)]">
                            Push to Teacher's Device <i class="fas fa-arrow-right ml-3"></i>
                        </button>
                    </div>"""
text = text.replace(old_f_controller_buttons, new_f_controller_buttons)

# Fix 3: Add Audio Logic to JS
# Inject into updateSlides()
old_update_slides = """document.getElementById('f-counter').innerText = currentSlide;"""
new_update_slides = """document.getElementById('f-counter').innerText = currentSlide;

        // Audio Logic: Pause everything first
        document.querySelectorAll('audio').forEach(audio => {
            audio.pause();
            audio.currentTime = 0;
        });
        
        // Show/hide the global audio button if the slide has an audio tag
        const audioBtn = document.getElementById('global-audio-btn');
        if (fSlide && fSlide.querySelector('audio')) {
            audioBtn.classList.remove('hidden');
            audioBtn.innerHTML = '<i class="fas fa-play mr-2"></i> Play Audio';
        } else {
            audioBtn.classList.add('hidden');
        }"""
text = text.replace(old_update_slides, new_update_slides)

# Add the new function
js_functions = """    function pushToParticipant() {"""
new_js_functions = """    function playCurrentSlideAudio() {
        let fId = currentSlide === -1 ? 'slide-lobby' : 'slide-' + currentSlide;
        const fSlide = document.getElementById(fId);
        if (fSlide) {
            const audio = fSlide.querySelector('audio');
            if (audio) {
                const btn = document.getElementById('global-audio-btn');
                if (audio.paused) {
                    audio.play();
                    btn.innerHTML = '<i class="fas fa-pause mr-2"></i> Pause Audio';
                    btn.classList.replace('bg-purple-600', 'bg-amber-500');
                    btn.classList.replace('hover:bg-purple-500', 'hover:bg-amber-400');
                    btn.classList.replace('shadow-[0_0_15px_rgba(147,51,234,0.5)]', 'shadow-[0_0_15px_rgba(245,158,11,0.5)]');
                } else {
                    audio.pause();
                    btn.innerHTML = '<i class="fas fa-play mr-2"></i> Play Audio';
                    btn.classList.replace('bg-amber-500', 'bg-purple-600');
                    btn.classList.replace('hover:bg-amber-400', 'hover:bg-purple-500');
                    btn.classList.replace('shadow-[0_0_15px_rgba(245,158,11,0.5)]', 'shadow-[0_0_15px_rgba(147,51,234,0.5)]');
                }
            }
        }
    }

    function pushToParticipant() {"""
text = text.replace(js_functions, new_js_functions)

# Also ensure we hide the actual default audio controls in the HTML to keep it clean (since we have a global button now)
text = text.replace('<audio controls ', '<audio ')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
