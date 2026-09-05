import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Task 1: Tab A Layout
old_tab_a_header = '<h2 class="text-3xl font-extrabold text-indigo-900 mb-6">SACE Auditor Guide</h2>'
new_tab_a_header = '''<div class="flex justify-between items-center mb-6">
    <h2 class="text-3xl font-extrabold text-indigo-900">SACE Auditor Program</h2>
    <button class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-black text-lg rounded-xl shadow-lg transition flex items-center" onclick="launchDemo()">
        Launch Full SACE Program <i class="fas fa-arrow-right ml-3"></i>
    </button>
</div>'''
text = text.replace(old_tab_a_header, new_tab_a_header)

old_btn_pattern = r'<button class="px-8 py-4 bg-indigo-600 hover:bg-indigo-700 text-white font-black text-xl rounded-xl shadow-lg transition flex items-center" onclick="launchDemo\(\)">.*?Launch Full SACE Program <i class="fas fa-arrow-right ml-3"></i>\s*</button>'
text = re.sub(old_btn_pattern, '', text, flags=re.DOTALL)

# Task 2: Remove App Synced
pattern_synced = r'<div class="bg-indigo-600 p-4 text-center text-white font-bold shadow-md z-10 flex justify-between items-center">\s*<span>AIT App</span>\s*<span class="text-xs bg-green-400 text-green-900 px-2 py-1 rounded-full"><i class="fas fa-link mr-1"></i>Synced</span>\s*</div>'
text = re.sub(pattern_synced, '', text)

# Task 4 & 5: Split the Views
old_view_10 = r'<div class="app-view hidden overflow-y-auto pb-20" id="app-view-10">.*?<div class="app-view hidden" id="app-view-12">'
new_views = '''<!-- Evaluation View (Slide 29) -->
<div class="app-view hidden overflow-y-auto pb-20" id="app-view-9">
  <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
    <h3 class="text-xl font-bold mb-1"><i class="fas fa-clipboard-check mr-2"></i>Evaluation of P and W/S</h3>
    <p class="text-xs text-indigo-200">Practical & Oral Evaluation</p>
  </div>
  
  <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-4">
    <h4 class="font-bold text-slate-800 mb-2 border-b pb-2">Evaluation of Facilitator</h4>
    <p class="text-xs text-slate-500 mb-4">Rate the physical demonstration of the LITRE sequence (0: Not demonstrated, 3: Clear/Strong)</p>
    <div class="space-y-2 text-sm">
      <div class="flex justify-between items-center"><span class="w-2/3">Clear vocalization</span> <select class="w-1/3 p-1 border rounded"><option value="3">3</option></select></div>
      <div class="flex justify-between items-center"><span class="w-2/3">Correct hand positioning</span> <select class="w-1/3 p-1 border rounded"><option value="3">3</option></select></div>
      <div class="flex justify-between items-center"><span class="w-2/3">Pacing for learners</span> <select class="w-1/3 p-1 border rounded"><option value="3">3</option></select></div>
    </div>
  </div>

  <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-6">
    <h4 class="font-bold text-slate-800 mb-2 border-b pb-2">Workshop Activities</h4>
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
  
  <button class="w-full py-4 bg-indigo-600 text-white font-bold rounded-lg shadow-lg hover:bg-indigo-700 transition" onclick="nextStep()">Proceed to Assessment <i class="fas fa-arrow-right ml-2"></i></button>
</div>

<!-- Assessment View (Slide 30) -->
<div class="app-view hidden overflow-y-auto pb-20" id="app-view-10">
  <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
    <h3 class="text-xl font-bold mb-1"><i class="fas fa-award mr-2"></i>Assessment</h3>
    <p class="text-xs text-indigo-200">Knowledge MCQ</p>
  </div>
  
  <form id="assessment-form" action="{{ url_for('sace_bp.submit_post_test') }}" method="POST">
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

<script>
function syncChecks(el) {
    const val = el.value;
    const mapping = {
        'Appropriate objective': 'comp_objective',
        'Correct sequence': 'comp_sequence',
        'Practical demo': 'comp_demo',
        'Learner participation': 'comp_participation',
        'Participant guidance': 'comp_guidance',
        'Reading practice': 'comp_reading',
        'Assessment': 'comp_assessment',
        'Reflection': 'comp_reflection'
    };
    const targetId = 'hidden_' + mapping[val];
    const targetEl = document.getElementById(targetId);
    if (targetEl) {
        targetEl.value = el.checked ? val : '';
    }
}
</script>

<div class="app-view hidden" id="app-view-12">'''
text = re.sub(old_view_10, new_views, text, flags=re.DOTALL)

# Update Counter JS and simSteps
old_simsteps = r'const simSteps = \[.*?\];'
new_simsteps = '''const simSteps = [
        { slide: 0, view: 'f' },
        { slide: 1, view: 'f' },
        { slide: 2, view: 'f' },
        { slide: 3, view: 'f' },
        { slide: 4, view: 'f' },
        { slide: 5, view: 'f' },
        { slide: 6, view: 'f' },
        { slide: 7, view: 'f' },
        { slide: 8, view: 'f' },
        { slide: 9, view: 'f' },
        { slide: 10, view: 'f' },
        { slide: 11, view: 'f' },
        { slide: 12, view: 'f' },
        { slide: 13, view: 'f' },
        { slide: 14, view: 'f' },
        { slide: 15, view: 'f' },
        { slide: 16, view: 'f' },
        { slide: 17, view: 'f' },
        { slide: 18, view: 'f' },
        { slide: 19, view: 'f' },
        { slide: 20, view: 'f' },
        { slide: 21, view: 'f' },
        { slide: 22, view: 'f' },
        { slide: 23, view: 'f' },
        { slide: 24, view: 'f' },
        { slide: 25, view: 'f' },
        { slide: 26, view: 'f' },
        { slide: 27, view: 'f' },
        { slide: 28, view: 'f' },
        { slide: 28, view: 'p', appView: 9 },
        { slide: 29, view: 'f' },
        { slide: 29, view: 'p', appView: 10 }
    ];'''
text = re.sub(old_simsteps, new_simsteps, text, flags=re.DOTALL)

# Add ID to counter container
text = text.replace('<div class="text-slate-500 font-mono text-sm font-bold border-r pr-4 border-slate-300">', 
                   '<div class="text-slate-500 font-mono text-sm font-bold border-r pr-4 border-slate-300" id="step-counter-container">')

# Update applyStep logic
old_counter_js = r'let displaySlide = step\.slide < 0 \? 0 : step\.slide \+ 1;\s*document\.getElementById\(\'f-counter-global\'\)\.innerText = displaySlide;'
new_counter_js = '''let displaySlide = step.slide < 0 ? 0 : step.slide + 1;
        if (step.appView === 9 || step.appView === 10 || step.slide === 28 || step.slide === 29) {
            document.getElementById('step-counter-container').innerText = "Post-Test";
        } else {
            document.getElementById('step-counter-container').innerHTML = `Step <span id="f-counter-global">${displaySlide}</span> of 30`;
        }'''
text = re.sub(old_counter_js, new_counter_js, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
