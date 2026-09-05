import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Let's fix the structural bug. The 	ab-f div should wrap the controller.
# We need to move the </div></div> that comes BEFORE <!-- Persistent F Controller --> 
# to AFTER the <!-- Persistent F Controller --> block.

old_block = '''
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-24">
              <img alt="Slide 25" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/25Thank.png') }}"/>
          </div>


</div>
</div>
<!-- Persistent F Controller -->
<div class="bg-slate-800 p-6 border-t border-slate-700 flex justify-between items-center shadow-lg z-50">
<div class="text-slate-400 font-mono text-lg font-bold">
                        Step <span id="f-counter">0</span> of 25
                    </div>
<div class="flex space-x-4 items-center">
<button class="px-6 py-4 bg-slate-300 hover:bg-slate-400 text-slate-700 font-bold rounded-xl transition" onclick="nextSlide()">
                                  Next Slide <i class="fas fa-chevron-right ml-2"></i>
                              </button>
                              <button class="hidden px-6 py-4 bg-purple-600 hover:bg-purple-500 text-white font-bold text-lg rounded-xl transition shadow-[0_0_15px_rgba(147,51,234,0.5)]" id="global-audio-btn" onclick="playCurrentSlideAudio()">
<i class="fas fa-play mr-2"></i> Play Audio
                        </button>
<button class="px-8 py-4 bg-emerald-600 hover:bg-emerald-500 text-white font-black text-xl rounded-xl transition shadow-[0_0_20px_rgba(16,185,129,0.5)]" onclick="pushToParticipant()">
                            Push to Participant's Device <i class="fas fa-arrow-right ml-3"></i>
</button>
</div>
</div>
</div>
<!-- ========================================== -->
<!-- TAB P: PARTICIPANT -->
'''

new_block = '''
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-24">
              <img alt="Slide 25" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/25Thank.png') }}"/>
          </div>

    </div> <!-- Close flex-grow -->

    <!-- Persistent F Controller -->
    <div class="bg-slate-800 p-6 border-t border-slate-700 flex justify-between items-center shadow-lg z-50 shrink-0">
        <div class="text-slate-400 font-mono text-lg font-bold">
            Step <span id="f-counter">0</span> of 25
        </div>
        <div class="flex space-x-4 items-center">
            <button class="px-6 py-4 bg-slate-300 hover:bg-slate-400 text-slate-700 font-bold rounded-xl transition" onclick="nextSlide()">
                Next Slide <i class="fas fa-chevron-right ml-2"></i>
            </button>
            <button class="hidden px-6 py-4 bg-purple-600 hover:bg-purple-500 text-white font-bold text-lg rounded-xl transition shadow-[0_0_15px_rgba(147,51,234,0.5)]" id="global-audio-btn" onclick="playCurrentSlideAudio()">
                <i class="fas fa-play mr-2"></i> Play Audio
            </button>
            <button class="px-8 py-4 bg-emerald-600 hover:bg-emerald-500 text-white font-black text-xl rounded-xl transition shadow-[0_0_20px_rgba(16,185,129,0.5)]" onclick="pushToParticipant()">
                Push to Participant's Device <i class="fas fa-arrow-right ml-3"></i>
            </button>
        </div>
    </div>
</div> <!-- Close tab-f -->

<!-- ========================================== -->
<!-- TAB P: PARTICIPANT -->
'''

text = text.replace(old_block, new_block)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

