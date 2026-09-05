import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

new_slides = """<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center" id="slide-0">
                        <img alt="Slide 1: Program" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/1Program.png') }}"/>
                    </div>
                    
                    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-1">
                        <img alt="Slide 2: Crisis" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/2Crisis.png') }}"/>
                    </div>
                    
                    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl" id="slide-2">
                        <img alt="Slide 3: Survey" class="max-h-[40vh] max-w-full object-contain mx-auto mb-6 shadow-sm rounded" src="{{ url_for('static', filename='sace_slides/3Survey.png') }}"/>
                        <h2 class="text-2xl font-bold text-indigo-600 mb-6"><i class="fas fa-poll mr-3"></i>Live Room Data Tally</h2>
                        <div class="w-full max-w-2xl space-y-6">
                            <!-- True Bar -->
                            <div>
                                <div class="flex justify-between text-sm font-bold text-slate-600 mb-1">
                                    <span>TRUE (Crisis Exists)</span>
                                    <span><span id="slide-2-true-pct">0</span>%</span>
                                </div>
                                <div class="w-full bg-slate-200 rounded-full h-4 overflow-hidden shadow-inner">
                                    <div class="bg-emerald-500 h-4 rounded-full transition-all duration-500" style="width: 0%"></div>
                                </div>
                            </div>
                            <!-- False Bar -->
                            <div>
                                <div class="flex justify-between text-sm font-bold text-slate-600 mb-1">
                                    <span>FALSE (No Crisis)</span>
                                    <span><span id="slide-2-false-pct">0</span>%</span>
                                </div>
                                <div class="w-full bg-slate-200 rounded-full h-4 overflow-hidden shadow-inner">
                                    <div class="bg-rose-500 h-4 rounded-full transition-all duration-500" style="width: 0%"></div>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-3">
                        <img alt="Slide 4: Study" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/4Study.png') }}"/>
                    </div>
                    
                    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-4">
                        <img alt="Slide 5: Problem" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/5Problem.png') }}"/>
                    </div>"""

# Find the block from <div id="slide-0"> down to right before <div id="slide-4"> ... Wait, we need to replace slide 0 to 4 in the old structure!
# The old structure had slide-0 up to slide-3. slide-4 was the root cause data tally.
# I will just regex replace the entire section.
# We want to replace from id="slide-0" to the end of id="slide-3", AND we shift the old slide-4 to slide-5, etc.
