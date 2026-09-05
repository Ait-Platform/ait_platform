import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Change main title
text = text.replace('<h1 class="text-2xl font-bold text-slate-800"><i class="fas fa-clipboard-list text-indigo-500 mr-2"></i> Post-Test</h1>', '<h1 class="text-2xl font-bold text-slate-800" id="main-title"><i class="fas fa-clipboard-check text-indigo-500 mr-2"></i> SACE Activity Evaluation</h1>')

# Flatten Step 1
old_step1_header = '''                <div class="bg-indigo-50 border border-indigo-200 p-6 rounded-xl mb-6">
                    <h3 class="font-bold text-indigo-900 mb-2 text-xl">Evaluation of Facilitator(s) Presentation</h3>
                    <p class="text-sm text-slate-600">Please rate the Facilitator (F) on the following aspects (0: Poor/Not demonstrated, 3: Clear/Strong). These scores are sent to the SACE Provider for quality assurance.</p>
                </div>'''
new_step1_header = '''                <div class="mb-6">
                    <h2 class="text-xl font-black text-slate-800 mb-2">Evaluation of Facilitator(s) Presentation</h2>
                    <p class="text-sm text-slate-500 border-b pb-4">Please rate the Facilitator (F) on the following aspects (0: Poor/Not demonstrated, 3: Clear/Strong). These scores are sent to the SACE Provider for quality assurance.</p>
                </div>'''
text = text.replace(old_step1_header, new_step1_header)

# Flatten Step 2
old_step2_header = '''                <div class="bg-indigo-50 border border-indigo-200 p-6 rounded-xl mb-6">
                    <h3 class="font-bold text-indigo-900 mb-2 text-xl">Evaluating W/S</h3>
                    <p class="text-sm text-slate-600">Please rate the Classroom Application (W/S) simulation.</p>
                </div>'''
new_step2_header = '''                <div class="mb-6">
                    <h2 class="text-xl font-black text-slate-800 mb-2">Evaluation of Classroom Application (W/S)</h2>
                    <p class="text-sm text-slate-500 border-b pb-4">Please rate the Classroom Application (W/S) simulation.</p>
                </div>'''
text = text.replace(old_step2_header, new_step2_header)

# Flatten Step 4
old_step4_header = '''                <div class="bg-emerald-50 border border-emerald-200 p-6 rounded-xl mb-6">
                    <h3 class="font-bold text-emerald-900 mb-2 text-xl">Post-Test Assessment</h3>
                    <p class="text-sm text-emerald-800">Final knowledge check. Select the best answer for each question.</p>
                </div>'''
new_step4_header = '''                <div class="mb-6">
                    <h2 class="text-xl font-black text-slate-800 mb-2">Final Post-Test Assessment</h2>
                    <p class="text-sm text-slate-500 border-b pb-4">Final knowledge check. Select the best answer for each question.</p>
                </div>'''
text = text.replace(old_step4_header, new_step4_header)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
