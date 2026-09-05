import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Fix F-board footer to be absolutely positioned so it NEVER gets pushed off screen
f_footer_old = '<div class="bg-slate-800 p-6 border-t border-slate-700 flex justify-between items-center shadow-lg z-50">'
f_footer_new = '<div class="absolute bottom-0 left-0 w-full bg-slate-800 p-6 border-t border-slate-700 flex justify-between items-center shadow-lg z-[100]">'
text = text.replace(f_footer_old, f_footer_new)

# 2. Fix P-board footer to be absolutely positioned
p_footer_old = '<div class="p-6 border-t border-slate-200 bg-white flex justify-between items-center shadow-[0_-5px_20px_rgba(0,0,0,0.1)] z-50">'
p_footer_new = '<div class="absolute bottom-0 left-0 w-full p-6 border-t border-slate-200 bg-white flex justify-between items-center shadow-[0_-5px_20px_rgba(0,0,0,0.1)] z-[100]">'
text = text.replace(p_footer_old, p_footer_new)

# 3. Add pb-32 to the flex-grow content areas so the absolute footers don't cover content
content_old = '<div class="flex-grow overflow-hidden relative">'
content_new = '<div class="flex-grow overflow-hidden relative pb-32">'
text = text.replace(content_old, content_new)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
