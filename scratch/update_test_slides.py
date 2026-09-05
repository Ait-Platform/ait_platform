import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Slide 31 (Evaluating F)
old_step1 = r'<!-- STEP 1: Section B \(Facilitator Rubric\) -->.*?<!-- STEP 2: Section C \(Classroom Application\) -->'
new_step1 = '''<!-- STEP 1: Evaluating F -->
            <div id="step-1" class="step-container">
                <div class="bg-indigo-50 border border-indigo-200 p-6 rounded-xl mb-6">
                    <h3 class="font-bold text-indigo-900 mb-2 text-xl">Evaluating F</h3>
                    <p class="text-sm text-slate-600">Please rate the Facilitator (F) on the following aspects (0: Poor/Not demonstrated, 3: Clear/Strong). These scores are sent to the SACE Provider for quality assurance.</p>
                </div>

                <div class="space-y-6">
                    <div class="flex flex-col">
                        <span class="font-semibold text-slate-800 mb-3">Explained concepts clearly</span>
                        <div class="flex space-x-3">
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_vocalization" value="0" class="mr-2" required> 0</label>
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_vocalization" value="1" class="mr-2"> 1</label>
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_vocalization" value="2" class="mr-2"> 2</label>
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_vocalization" value="3" class="mr-2"> 3</label>
                        </div>
                    </div>
                    <div class="flex flex-col">
                        <span class="font-semibold text-slate-800 mb-3">Demonstrated the LITRE sequence accurately</span>
                        <div class="flex space-x-3">
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_positioning" value="0" class="mr-2" required> 0</label>
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_positioning" value="1" class="mr-2"> 1</label>
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_positioning" value="2" class="mr-2"> 2</label>
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_positioning" value="3" class="mr-2"> 3</label>
                        </div>
                    </div>
                    <div class="flex flex-col">
                        <span class="font-semibold text-slate-800 mb-3">Maintained good pacing for adult learners</span>
                        <div class="flex space-x-3">
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_pacing" value="0" class="mr-2" required> 0</label>
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_pacing" value="1" class="mr-2"> 1</label>
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_pacing" value="2" class="mr-2"> 2</label>
                            <label class="flex-1 text-center py-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-indigo-50 transition"><input type="radio" name="rubric_pacing" value="3" class="mr-2"> 3</label>
                        </div>
                    </div>
                </div>

                <div class="pt-8 text-right border-t border-slate-100 mt-8">
                    <button type="button" onclick="goToStep(2)" class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow transition">
                        Next: Evaluating W/S <i class="fas fa-arrow-right ml-2"></i>
                    </button>
                </div>
            </div>

            <!-- STEP 2: Evaluating W/S -->'''

text = re.sub(old_step1, new_step1, text, flags=re.DOTALL)

# 2. Slide 32 (Evaluating W/S) & Removing Back buttons
old_step2 = r'<!-- STEP 2: Evaluating W/S -->.*?<!-- STEP 3: Post-Test \(MCQ\) -->'
new_step2 = '''<!-- STEP 2: Evaluating W/S -->
            <div id="step-2" class="step-container hidden">
                <div class="bg-teal-50 border border-teal-200 p-6 rounded-xl mb-6">
                    <h3 class="font-bold text-teal-900 mb-2 text-xl">Evaluating W/S</h3>
                    <p class="text-sm text-teal-800">Please indicate Yes or No for the following activities. The activities marked 'Yes' will be officially printed on your SACE Certificate as Competencies Achieved.</p>
                </div>

                <div class="space-y-4 text-sm font-medium text-slate-700">
                    <div class="flex items-center justify-between bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                        <span>Appropriate objective</span>
                        <div class="flex space-x-4">
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_objective" value="Appropriate objective" class="mr-2 h-5 w-5 text-teal-600" required> Yes</label>
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_objective" value="" class="mr-2 h-5 w-5 text-slate-400"> No</label>
                        </div>
                    </div>
                    <div class="flex items-center justify-between bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                        <span>Correct sequence</span>
                        <div class="flex space-x-4">
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_sequence" value="Correct sequence" class="mr-2 h-5 w-5 text-teal-600" required> Yes</label>
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_sequence" value="" class="mr-2 h-5 w-5 text-slate-400"> No</label>
                        </div>
                    </div>
                    <div class="flex items-center justify-between bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                        <span>Practical demo</span>
                        <div class="flex space-x-4">
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_demo" value="Practical demo" class="mr-2 h-5 w-5 text-teal-600" required> Yes</label>
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_demo" value="" class="mr-2 h-5 w-5 text-slate-400"> No</label>
                        </div>
                    </div>
                    <div class="flex items-center justify-between bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                        <span>Learner participation</span>
                        <div class="flex space-x-4">
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_participation" value="Learner participation" class="mr-2 h-5 w-5 text-teal-600" required> Yes</label>
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_participation" value="" class="mr-2 h-5 w-5 text-slate-400"> No</label>
                        </div>
                    </div>
                    <div class="flex items-center justify-between bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                        <span>Participant guidance</span>
                        <div class="flex space-x-4">
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_guidance" value="Participant guidance" class="mr-2 h-5 w-5 text-teal-600" required> Yes</label>
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_guidance" value="" class="mr-2 h-5 w-5 text-slate-400"> No</label>
                        </div>
                    </div>
                    <div class="flex items-center justify-between bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                        <span>Reading practice</span>
                        <div class="flex space-x-4">
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_reading" value="Reading practice" class="mr-2 h-5 w-5 text-teal-600" required> Yes</label>
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_reading" value="" class="mr-2 h-5 w-5 text-slate-400"> No</label>
                        </div>
                    </div>
                    <div class="flex items-center justify-between bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                        <span>Assessment</span>
                        <div class="flex space-x-4">
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_assessment" value="Assessment" class="mr-2 h-5 w-5 text-teal-600" required> Yes</label>
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_assessment" value="" class="mr-2 h-5 w-5 text-slate-400"> No</label>
                        </div>
                    </div>
                    <div class="flex items-center justify-between bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                        <span>Reflection</span>
                        <div class="flex space-x-4">
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_reflection" value="Reflection" class="mr-2 h-5 w-5 text-teal-600" required> Yes</label>
                            <label class="cursor-pointer flex items-center"><input type="radio" name="comp_reflection" value="" class="mr-2 h-5 w-5 text-slate-400"> No</label>
                        </div>
                    </div>
                </div>

                <div class="pt-8 text-right border-t border-slate-100 mt-8">
                    <button type="button" onclick="goToStep(3)" class="px-8 py-3 bg-teal-600 hover:bg-teal-700 text-white font-bold rounded-lg shadow transition">
                        Next: Post-Test <i class="fas fa-arrow-right ml-2"></i>
                    </button>
                </div>
            </div>

            <!-- STEP 3: Post-Test (MCQ) -->'''

text = re.sub(old_step2, new_step2, text, flags=re.DOTALL)

# Remove the back button from Step 3
old_step3_back = r'<button type="button" onclick="goToStep\(2\)" class="px-6 py-3 bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold rounded-lg transition">\s*<i class="fas fa-arrow-left mr-2"></i> Back\s*</button>'
text = re.sub(old_step3_back, '', text)
text = text.replace('pt-8 flex justify-between border-t', 'pt-8 text-right border-t')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
