import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the F Controller
old_f_controller = """<!-- Persistent F Controller -->
                <div class="bg-slate-800 p-4 border-t border-slate-700 flex justify-between items-center shadow-lg z-50">
                    <button onclick="prevFSlide()" class="px-6 py-3 bg-slate-700 hover:bg-slate-600 text-white font-bold rounded-lg transition">
                        <i class="fas fa-chevron-left mr-2"></i> Prev Slide
                    </button>
                    <div class="text-center text-slate-400 font-mono">
                        Slide <span id="f-counter">0</span> / 11
                    </div>
                    <div class="flex space-x-3">
                        <button onclick="peekParticipant()" class="px-6 py-3 bg-indigo-600 hover:bg-indigo-500 text-white font-bold rounded-lg transition shadow-[0_0_15px_rgba(79,70,229,0.5)]">
                            <i class="fas fa-mobile-alt mr-2"></i> View Teacher's Device
                        </button>
                        <button onclick="nextFSlide()" class="px-6 py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-lg transition shadow-[0_0_15px_rgba(16,185,129,0.5)]">
                            Next Slide <i class="fas fa-chevron-right ml-2"></i>
                        </button>
                    </div>
                </div>"""

new_f_controller = """<!-- Persistent F Controller -->
                <div class="bg-slate-800 p-6 border-t border-slate-700 flex justify-between items-center shadow-lg z-50">
                    <div class="text-slate-400 font-mono text-lg font-bold">
                        Step <span id="f-counter">0</span> of 11
                    </div>
                    <button onclick="pushToParticipant()" class="px-8 py-4 bg-emerald-600 hover:bg-emerald-500 text-white font-black text-xl rounded-xl transition shadow-[0_0_20px_rgba(16,185,129,0.5)]">
                        Push to Teacher's Device <i class="fas fa-arrow-right ml-3"></i>
                    </button>
                </div>"""
text = text.replace(old_f_controller, new_f_controller)

# Replace the P Controller
old_p_controller = """<!-- Persistent P Return -->
                <div class="p-4 border-t border-slate-200 bg-white text-center shadow-[0_-5px_15px_rgba(0,0,0,0.05)] z-50">
                    <button onclick="showTab('f')" class="px-8 py-3 bg-slate-800 hover:bg-slate-700 text-white font-bold rounded-lg transition">
                        <i class="fas fa-undo mr-2"></i> Return to Facilitator View
                    </button>
                </div>"""

new_p_controller = """<!-- Persistent P Return -->
                <div class="p-6 border-t border-slate-200 bg-white flex justify-between items-center shadow-[0_-5px_20px_rgba(0,0,0,0.1)] z-50">
                    <div class="text-slate-400 font-mono text-lg font-bold"><i class="fas fa-mobile-alt mr-2"></i> Teacher View Active</div>
                    <button onclick="finishActivityAndNext()" class="px-8 py-4 bg-indigo-600 hover:bg-indigo-700 text-white font-black text-xl rounded-xl transition shadow-[0_0_15px_rgba(79,70,229,0.5)]">
                        Next Facilitator Slide <i class="fas fa-undo ml-3"></i>
                    </button>
                </div>"""
text = text.replace(old_p_controller, new_p_controller)


# Replace JS functions
js_to_replace = """    function prevFSlide() {
        if (currentSlide > 0) {
            currentSlide--;
            updateSlides();
        }
    }

    function nextFSlide() {
        if (currentSlide < totalSlides) {
            currentSlide++;
            updateSlides();
            
            // Random Dice Roll for mock surveys
            triggerRandomDice(currentSlide);
            
            // Auto-pop the P tab if it's an interactive slide to show cause and effect
            if ([0, 3, 4, 5, 8, 9, 10, 11].includes(currentSlide)) {
                setTimeout(() => {
                    peekParticipant();
                }, 1500);
            }
        }
    }

    function peekParticipant() {
        showTab('p');
    }"""

new_js = """    function pushToParticipant() {
        showTab('p');
    }

    function finishActivityAndNext() {
        if (currentSlide < totalSlides) {
            currentSlide++;
            updateSlides();
            triggerRandomDice(currentSlide);
            showTab('f');
        } else {
            alert("Simulation Complete! Endorsement demonstration finished.");
            showTab('a');
        }
    }"""
text = text.replace(js_to_replace, new_js)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
