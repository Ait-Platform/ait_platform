import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Flatten Step 2
old_step_2 = '''<div class="bg-teal-50 border border-teal-200 p-6 rounded-xl mb-6">
                    <h3 class="font-bold text-teal-900 mb-2 text-xl">Evaluating W/S</h3>
                    <p class="text-sm text-teal-800">Please indicate Yes or No for the following activities. The activities marked 'Yes' will be officially printed on your SACE Certificate as Competencies Achieved.</p>
                </div>'''

new_step_2 = '''<div class="mb-6">
                    <h2 class="text-xl font-black text-slate-800 mb-2">Evaluating W/S</h2>
                    <p class="text-sm text-slate-500 border-b pb-4">Please indicate Yes or No for the following activities. The activities marked 'Yes' will be officially printed on your Certificate as Competencies Achieved.</p>
                </div>'''
text = text.replace(old_step_2, new_step_2)

# 2. Flatten Step 3
old_step_3_start = '''<div class="bg-purple-50 border border-purple-200 p-8 rounded-xl mb-6 shadow-sm">
                    <h3 class="font-black text-purple-900 mb-4 text-2xl"><i class="fas fa-microscope mr-2"></i> Post-Workshop Survey (Longitudinal Study)</h3>
                    
                    <div class="bg-white p-6 rounded-lg border border-purple-100 text-purple-800 leading-relaxed italic mb-6">
                        "If you watch Snow White today, or 50 years from now, the quality must remain exactly the same." — Inspired by Walt Disney
                        <br><br>
                        <strong>Why we need your interaction:</strong> We are striving for that same timeless standard of quality with the LITRE method. This brief survey is not an assessment of you, but rather a vital data collection point for our longitudinal path analysis. We are tracking whether LITRE truly holds its value and efficacy over time within the schooling system. Your honest feedback here provides our research baseline.
                    </div>
                </div>'''

new_step_3 = '''<div class="mb-6">
                    <h2 class="text-xl font-black text-slate-800 mb-2">Post-Workshop Survey (Longitudinal Study)</h2>
                    <p class="text-sm text-slate-500 border-b pb-4">We are tracking whether LITRE truly holds its value and efficacy over time within the schooling system. Your honest feedback here provides our research baseline.</p>
                </div>
                <div class="bg-indigo-50 border-l-4 border-indigo-500 p-6 rounded-r-lg text-indigo-900 leading-relaxed italic mb-8 shadow-sm">
                    "If you watch Snow White today, or 50 years from now, the quality must remain exactly the same." — Inspired by Walt Disney
                </div>'''

# The Walt Disney quote character was actually a unicode dash. Let's use regex to be safe.
pattern_step3 = r'<div class="bg-purple-50 border border-purple-200 p-8 rounded-xl mb-6 shadow-sm">.*?</div>\s*</div>'
text = re.sub(pattern_step3, new_step_3, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
