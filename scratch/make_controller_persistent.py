import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Strip out the old controller
text = re.sub(
    r'<!-- Persistent F Controller -->.*?Push to Participant\'s Device.*?</div>\n</div>',
    '',
    text,
    flags=re.DOTALL
)

# 2. Add the global controller before <script>
global_controller = '''
    <!-- Persistent Global Controller -->
    <div class="bg-slate-800 p-4 md:p-6 border-t border-slate-700 flex justify-between items-center shadow-[0_-10px_20px_rgba(0,0,0,0.3)] z-[100] shrink-0" id="global-controller" style="display: none;">
        <div class="text-slate-400 font-mono text-lg font-bold">
            Step <span id="f-counter-global">0</span> of 25
        </div>
        <div class="flex space-x-4 items-center">
            <button class="px-4 md:px-6 py-3 bg-slate-300 hover:bg-slate-400 text-slate-700 font-bold rounded-xl transition" onclick="nextSlide()">
                Next Slide <i class="fas fa-chevron-right ml-2"></i>
            </button>
            <button class="hidden px-4 md:px-6 py-3 bg-purple-600 hover:bg-purple-500 text-white font-bold rounded-xl transition shadow-[0_0_15px_rgba(147,51,234,0.5)]" id="global-audio-btn" onclick="playCurrentSlideAudio()">
                <i class="fas fa-play mr-2"></i> Play Audio
            </button>
            <button class="px-4 md:px-8 py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-black rounded-xl transition shadow-[0_0_20px_rgba(16,185,129,0.5)]" onclick="pushToParticipant()">
                Push to Participant <i class="fas fa-arrow-right ml-2"></i>
            </button>
        </div>
    </div>
</div>
'''

text = text.replace('</div>\n\n<script>', global_controller + '\n<script>')
text = text.replace('</div>\n<script>', global_controller + '\n<script>')
text = text.replace("document.getElementById('f-counter')", "document.getElementById('f-counter-global')")

# 3. Update nextSlide() to showTab('f')
text = text.replace('currentSlide++;\n            updateSlides();\n        } else {', 'currentSlide++;\n            updateSlides();\n            showTab("f");\n        } else {')

# 4. Show global controller when launching demo
text = text.replace('showTab(\'f\');\n    }', 'showTab(\'f\');\n        document.getElementById("global-controller").style.display = "flex";\n    }')

# 5. In pushToParticipant(), we can also force showTab('p')
# It's already doing it: showTab('p');

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

