import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

section_c = '''
            <!-- Section C: Classroom Application -->
            <div class="bg-slate-50 p-6 rounded-xl border border-slate-200">
                <h3 class="text-lg font-bold text-slate-800 mb-2 border-b pb-2">Classroom Application</h3>
                <p class="text-sm text-slate-500 mb-4">Check all that you successfully demonstrated or included in the workshop activity.</p>
                <div class="grid grid-cols-2 gap-3 text-sm font-medium text-slate-700">
                    <label class="flex items-center space-x-3 bg-white p-3 rounded border border-slate-200 cursor-pointer hover:bg-indigo-50">
                        <input name="comp_objective" type="checkbox" value="Appropriate objective" class="h-5 w-5 text-indigo-600 rounded"><span>Appropriate objective</span>
                    </label>
                    <label class="flex items-center space-x-3 bg-white p-3 rounded border border-slate-200 cursor-pointer hover:bg-indigo-50">
                        <input name="comp_sequence" type="checkbox" value="Correct sequence" class="h-5 w-5 text-indigo-600 rounded"><span>Correct sequence</span>
                    </label>
                    <label class="flex items-center space-x-3 bg-white p-3 rounded border border-slate-200 cursor-pointer hover:bg-indigo-50">
                        <input name="comp_demo" type="checkbox" value="Practical demo" class="h-5 w-5 text-indigo-600 rounded"><span>Practical demo</span>
                    </label>
                    <label class="flex items-center space-x-3 bg-white p-3 rounded border border-slate-200 cursor-pointer hover:bg-indigo-50">
                        <input name="comp_participation" type="checkbox" value="Learner participation" class="h-5 w-5 text-indigo-600 rounded"><span>Learner participation</span>
                    </label>
                    <label class="flex items-center space-x-3 bg-white p-3 rounded border border-slate-200 cursor-pointer hover:bg-indigo-50">
                        <input name="comp_guidance" type="checkbox" value="Participant guidance" class="h-5 w-5 text-indigo-600 rounded"><span>Participant guidance</span>
                    </label>
                    <label class="flex items-center space-x-3 bg-white p-3 rounded border border-slate-200 cursor-pointer hover:bg-indigo-50">
                        <input name="comp_reading" type="checkbox" value="Reading practice" class="h-5 w-5 text-indigo-600 rounded"><span>Reading practice</span>
                    </label>
                    <label class="flex items-center space-x-3 bg-white p-3 rounded border border-slate-200 cursor-pointer hover:bg-indigo-50">
                        <input name="comp_assessment" type="checkbox" value="Assessment" class="h-5 w-5 text-indigo-600 rounded"><span>Assessment</span>
                    </label>
                    <label class="flex items-center space-x-3 bg-white p-3 rounded border border-slate-200 cursor-pointer hover:bg-indigo-50">
                        <input name="comp_reflection" type="checkbox" value="Reflection" class="h-5 w-5 text-indigo-600 rounded"><span>Reflection</span>
                    </label>
                </div>
            </div>

            <div class="pt-6 border-t border-slate-200">
'''

text = text.replace('<div class="pt-6 border-t border-slate-200">', section_c)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

