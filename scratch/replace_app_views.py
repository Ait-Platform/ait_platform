import os

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_html = '''<!-- Section B: Facilitator Evaluation (Slide 31) -->
<div class="app-view hidden overflow-y-auto pb-20" id="app-view-31">
  <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
    <h3 class="text-xl font-bold mb-1"><i class="fas fa-clipboard-check mr-2"></i>Evaluation of SACE Program</h3>
    <p class="text-xs text-indigo-200">Evaluation of Facilitator</p>
  </div>
  
  <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-6">
    <h4 class="font-bold text-slate-800 mb-2 border-b pb-2">Practical Rubric</h4>
    <p class="text-xs text-slate-500 mb-4">Rate the physical demonstration of the LITRE sequence (0: Not demonstrated, 3: Clear/Strong)</p>
    <div class="space-y-4 text-sm">
      <div class="flex justify-between items-center"><span class="w-2/3">Clear vocalization</span> <select class="w-1/3 p-1 border rounded bg-slate-50"><option value="3">3</option></select></div>
      <div class="flex justify-between items-center"><span class="w-2/3">Correct hand positioning</span> <select class="w-1/3 p-1 border rounded bg-slate-50"><option value="3">3</option></select></div>
      <div class="flex justify-between items-center"><span class="w-2/3">Pacing for learners</span> <select class="w-1/3 p-1 border rounded bg-slate-50"><option value="3">3</option></select></div>
    </div>
  </div>
  <button class="w-full py-4 bg-indigo-600 text-white font-bold rounded-lg shadow-lg hover:bg-indigo-700 transition" onclick="nextStep()">Proceed to Next Evaluation <i class="fas fa-arrow-right ml-2"></i></button>
</div>

<!-- Section C: Workshop Activities (Slide 32) -->
<div class="app-view hidden overflow-y-auto pb-20" id="app-view-32">
  <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
    <h3 class="text-xl font-bold mb-1"><i class="fas fa-tasks mr-2"></i>Evaluation of Participant</h3>
    <p class="text-xs text-indigo-200">Workshop Activities</p>
  </div>

  <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-6">
    <h4 class="font-bold text-slate-800 mb-2 border-b pb-2">Practical & Oral Participation</h4>
    <p class="text-xs text-slate-500 mb-4">Check all that are successfully included in the participant's practical/oral participation.</p>
    <div class="grid grid-cols-2 gap-2 text-xs font-medium text-slate-700">
      <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Appropriate objective" onchange="syncChecks(this)" /><span>Appropriate objective</span></label>
      <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Correct sequence" onchange="syncChecks(this)" /><span>Correct sequence</span></label>
      <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Practical demo" onchange="syncChecks(this)" /><span>Practical demo</span></label>
      <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Learner participation" onchange="syncChecks(this)" /><span>Learner participation</span></label>
      <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Participant guidance" onchange="syncChecks(this)" /><span>Participant guidance</span></label>
      <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Reading practice" onchange="syncChecks(this)" /><span>Reading practice</span></label>
      <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Assessment" onchange="syncChecks(this)" /><span>Assessment</span></label>
      <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input class="application-check" type="checkbox" value="Reflection" onchange="syncChecks(this)" /><span>Reflection</span></label>
    </div>
  </div>
  
  <button class="w-full py-4 bg-indigo-600 text-white font-bold rounded-lg shadow-lg hover:bg-indigo-700 transition" onclick="nextStep()">Proceed to Post-Test Assessment <i class="fas fa-arrow-right ml-2"></i></button>
</div>

<!-- Section A: Assessment View (Slide 33) -->
<div class="app-view hidden overflow-y-auto pb-20" id="app-view-33">
  <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
    <h3 class="text-xl font-bold mb-1"><i class="fas fa-award mr-2"></i>Post-Test Assessment</h3>
    <p class="text-xs text-indigo-200">Knowledge MCQ</p>
  </div>
  
  <form id="assessment-form" action="{{ url_for('sace_bp.submit_post_test') }}" method="POST">
    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
    <input type="hidden" name="comp_objective" id="hidden_comp_objective" value="">
    <input type="hidden" name="comp_sequence" id="hidden_comp_sequence" value="">
    <input type="hidden" name="comp_demo" id="hidden_comp_demo" value="">
    <input type="hidden" name="comp_participation" id="hidden_comp_participation" value="">
    <input type="hidden" name="comp_guidance" id="hidden_comp_guidance" value="">
    <input type="hidden" name="comp_reading" id="hidden_comp_reading" value="">
    <input type="hidden" name="comp_assessment" id="hidden_comp_assessment" value="">
    <input type="hidden" name="comp_reflection" id="hidden_comp_reflection" value="">

    <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-4">
      <div class="mb-4">
        <p class="font-semibold text-sm mb-2 text-slate-700">1. What is the purpose of the LITRE blending-machine concept?</p>
        <div class="space-y-1 text-sm">
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q1" required type="radio" value="A"/> A. To replace reading practice</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q1" type="radio" value="B"/> B. To provide a physical and visual representation of the blending process</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q1" type="radio" value="C"/> C. To test handwriting</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q1" type="radio" value="D"/> D. To teach mathematical concepts</label>
        </div>
      </div>
      <div class="mb-4">
        <p class="font-semibold text-sm mb-2 text-slate-700">2. What does the palm represent in the LITRE model?</p>
        <div class="space-y-1 text-sm">
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q2" required type="radio" value="A"/> A. The classroom</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q2" type="radio" value="B"/> B. The meeting place in the blending model</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q2" type="radio" value="C"/> C. The alphabet</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q2" type="radio" value="D"/> D. The worksheet</label>
        </div>
      </div>
      <div class="mb-4">
        <p class="font-semibold text-sm mb-2 text-slate-700">3. What is the Participant's role during guided practice?</p>
        <div class="space-y-1 text-sm">
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q3" required type="radio" value="A"/> A. Provide all answers</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q3" type="radio" value="B"/> B. Allow learners to work without guidance</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q3" type="radio" value="C"/> C. Demonstrate, guide, observe and provide corrective support</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q3" type="radio" value="D"/> D. Only mark worksheets</label>
        </div>
      </div>
      <div class="mb-4">
        <p class="font-semibold text-sm mb-2 text-slate-700">4. Why is repetition important in catch-up reading?</p>
        <div class="space-y-1 text-sm">
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q4" required type="radio" value="A"/> A. It provides additional opportunities for learners to practise and consolidate skills.</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q4" type="radio" value="B"/> B. It eliminates the need for teaching.</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q4" type="radio" value="C"/> C. It prevents learner participation.</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2" name="q4" type="radio" value="D"/> D. It replaces assessment.</label>
        </div>
      </div>
    </div>
    <button type="submit" class="w-full py-4 bg-green-600 hover:bg-green-700 text-white text-xl font-black rounded-xl shadow-lg transition transform hover:-translate-y-1">
        Submit Answers & Claim Certificate <i class="fas fa-arrow-right ml-2"></i>
    </button>
  </form>
</div>
'''

new_lines = lines[:199] + [new_html + '\n'] + lines[502:]

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
