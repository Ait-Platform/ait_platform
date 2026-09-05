import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Disable clicking on F and P tabs, and make them look like unclickable indicators
tab_f_old = '<button class="flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 hover:opacity-100 hover:bg-slate-700" id="btn-tab-f" onclick="showTab(\\\'f\\\')">'
tab_f_new = '<div class="flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 cursor-default" id="btn-tab-f">'
# We will use regex for safety on the exact class string since we modify it in JS too
text = re.sub(r'<button class="[^"]*" id="btn-tab-f" onclick="showTab\(\'f\'\)">', 
              r'<div class="flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 cursor-default" id="btn-tab-f">', text)

text = re.sub(r'<button class="[^"]*" id="btn-tab-p" onclick="showTab\(\'p\'\)">', 
              r'<div class="flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 cursor-default" id="btn-tab-p">', text)

# We also need to change the closing tags from </button> to </div> for btn-tab-f and p
# The HTML block is:
# <button ... id="btn-tab-f" ...>
#     <div ...></div> Facilitator (F)
# </button>
# It's tricky to regex just the closing tag. I'll do a simple string replace for that specific block.
text = text.replace('Facilitator (F)\n            </button>', 'Facilitator (F)\n            </div>')
text = text.replace('Participant (P)\n            </button>', 'Participant (P)\n            </div>')

# Update JS inactiveClass to not have hover states since it's a div now
old_inactive = 'const inactiveClass = "flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 hover:opacity-100 hover:bg-slate-700";'
new_inactive = 'const inactiveClass = "flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 cursor-default";'
text = text.replace(old_inactive, new_inactive)

# 2. Add the "Next Slide" button back to the F-board footer (but NO Prev Slide)
footer_search = '<button class="hidden px-6 py-4 bg-purple-600 hover:bg-purple-500 text-white font-bold text-lg rounded-xl transition shadow-[0_0_15px_rgba(147,51,234,0.5)]" id="global-audio-btn" onclick="playCurrentSlideAudio()">'

footer_replace = '''<button class="px-6 py-4 bg-slate-300 hover:bg-slate-400 text-slate-700 font-bold rounded-xl transition" onclick="nextSlide()">
                                  Next Slide <i class="fas fa-chevron-right ml-2"></i>
                              </button>
                              ''' + footer_search

if 'onclick="nextSlide()"' not in text:
    text = text.replace(footer_search, footer_replace)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
