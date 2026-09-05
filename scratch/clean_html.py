import re

# Clean F slides
with open('scratch/raw_f_slides.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Cut off at the end of slide-11
match = re.search(r'(<div id="slide-11".*?</span>\s*</div>\s*</div>)', text, re.DOTALL)
if match:
    # Everything up to the end of slide 11
    pos = text.find('<div id="slide-11"')
    text = text[:pos] + match.group(1)
else:
    # Just cut off before the facilitator note
    pos = text.find('<div class="p-4 bg-indigo-50 border border-indigo-200 shadow-sm rounded-lg">')
    if pos != -1:
        text = text[:pos]
        # Also trim trailing </div>s
        text = re.sub(r'(</div>\s*)+$', '', text)
        
with open('scratch/raw_f_slides_clean.html', 'w', encoding='utf-8') as f:
    f.write(text)

# Clean P views
with open('scratch/raw_p_views.html', 'r', encoding='utf-8') as f:
    ptext = f.read()

pos = ptext.find('<div id="assessment-result-container"')
if pos != -1:
    # Cut off appropriately for slide-11 (assessment)
    ptext = ptext[:pos] + '<div id="assessment-result-container" class="bg-white p-6 rounded-xl shadow-lg border border-slate-200 text-center mt-10"><i class="fas fa-spinner fa-spin text-4xl text-indigo-500 mb-4"></i><p class="text-slate-600">Awaiting assessment results...</p></div></div>'

with open('scratch/raw_p_views_clean.html', 'w', encoding='utf-8') as f:
    f.write(ptext)
