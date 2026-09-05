import re
file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Strip out the bottom global controller completely
pattern = r'<!-- Persistent Global Controller -->.*?</div>\n</div>\n\n<script>'
text = re.sub(pattern, '</div>\n\n<script>', text, flags=re.DOTALL)

# 2. Re-insert the Next button, Audio, and Counter into the Top Tab Bar
tabs_pattern = r'<!-- Traffic Robot Tabs -->\n\s*<div class="flex space-x-2 border-b-2 border-slate-700 pb-0 bg-slate-50 pt-2 px-4 shadow-inner z-10">.*?</div>'

new_tabs = '''<!-- Traffic Robot Tabs -->
    <div class="flex justify-between items-end border-b-2 border-slate-700 pb-0 bg-slate-50 pt-2 px-4 shadow-inner z-10">
        <div class="flex space-x-2">
            <button class="flex items-center px-6 py-3 bg-indigo-600 text-white font-bold rounded-t-lg transition border-b-2 border-indigo-400" id="btn-tab-a" onclick="showTab('a')">
                <i class="fas fa-book-open mr-2"></i> SACE Auditor (A)
            </button>
            <button class="flex items-center px-6 py-3 bg-slate-200 text-slate-600 font-bold rounded-t-lg transition border-b-2 border-slate-300 hover:bg-slate-300" id="btn-tab-f" onclick="showTab('f')">
                <div class="w-3 h-3 rounded-full bg-red-600 shadow-[0_0_8px_rgba(220,38,38,0.5)] mr-3" id="light-f"></div> Facilitator (F)
            </button>
            <button class="flex items-center px-6 py-3 bg-slate-200 text-slate-600 font-bold rounded-t-lg transition border-b-2 border-slate-300 hover:bg-slate-300" id="btn-tab-p" onclick="showTab('p')">
                <div class="w-3 h-3 rounded-full bg-red-600 shadow-[0_0_8px_rgba(220,38,38,0.5)] mr-3" id="light-p"></div> Participant (P)
            </button>
        </div>
        
        <div class="flex space-x-4 items-center pb-2" id="global-controller" style="display: none;">
            <div class="text-slate-500 font-mono text-sm font-bold border-r pr-4 border-slate-300">
                Step <span id="f-counter-global">0</span> of 28
            </div>
            <button class="hidden px-4 py-2 bg-purple-600 hover:bg-purple-500 text-white font-bold rounded-lg transition shadow-[0_0_15px_rgba(147,51,234,0.5)] text-sm" id="global-audio-btn" onclick="playCurrentSlideAudio()">
                <i class="fas fa-play mr-2"></i> Audio
            </button>
            <button class="px-6 py-2 bg-indigo-600 hover:bg-indigo-500 text-white font-black text-lg rounded-lg transition shadow-[0_0_15px_rgba(79,70,229,0.5)]" id="global-next-btn" onclick="nextStep()">
                Next <i class="fas fa-arrow-right ml-2"></i>
            </button>
        </div>
    </div>'''

text = re.sub(tabs_pattern, new_tabs, text, flags=re.DOTALL)

# 3. Strip out the "Next Facilitator Slide" footer bar in P tab completely
# Look for <div class="p-6 border-t border-slate-200 bg-white flex justify-between items-center shadow-[0_-5px_20px_rgba(0,0,0,0.1)] z-50">
p_footer = r'<div class="p-6 border-t border-slate-200 bg-white flex justify-between items-center shadow-\[0_-5px_20px_rgba\(0,0,0,0\.1\)\] z-50">.*?</div>\n\s*</div>'
text = re.sub(p_footer, '</div>', text, flags=re.DOTALL)

# 4. Modify mockPoll and submitLog to just call nextStep()
text = text.replace('function mockPoll(type) {\n        alert("Participant response recorded locally! In the live app, this saves to PostgreSQL.");\n    }', 'function mockPoll(type) {\n        nextStep();\n    }')

# wait, submitLog isn't defined? I need to check where submitLog is. Let's see if submitLog is in JS or just in the onclicks.

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
