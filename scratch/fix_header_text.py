import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Fix the paragraph text
text = text.replace('the <strong>I Learn to Read English Using the LITRE Method</strong> intervention program', 'the <strong>I Learn to Read English Using the LITRE Method</strong>')

# Fix the header to be two lines of the same h1
old_header = '''                <h1 class="text-xl font-black text-slate-800 tracking-tight">
                    <i class="fas fa-shield-alt text-indigo-600 mr-2"></i> ARCHONEY INSTITUTE OF TECHNOLOGY (AIT)
                </h1>
                <p class="text-slate-500 font-bold mt-1 text-sm uppercase tracking-wide">Provider Activity: I Learn to Read English Using the LITRE Method</p>'''

new_header = '''                <h1 class="text-2xl font-black text-slate-800 tracking-tight leading-snug">
                    ARCHONEY INSTITUTE OF TECHNOLOGY (AIT)<br>
                    <span class="text-lg text-slate-600">Provider Activity: I Learn to Read English Using the LITRE Method</span>
                </h1>'''

text = text.replace(old_header, new_header)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
