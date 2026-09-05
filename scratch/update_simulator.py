import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Make headers bigger for Slide 31 and 32
html = html.replace('<h3 class="text-xl font-bold mb-1 text-slate-800"><i class="fas fa-clipboard-check text-indigo-500 mr-2"></i>Evaluation of Facilitator</h3>', 
                    '<h3 class="text-2xl font-black mb-1 text-slate-800"><i class="fas fa-clipboard-check text-indigo-500 mr-2"></i>Evaluation of Facilitator (Slide 31)</h3>')

html = html.replace('<h3 class="text-xl font-bold mb-1 text-slate-800"><i class="fas fa-tasks text-indigo-500 mr-2"></i>Classroom Application</h3>', 
                    '<h3 class="text-2xl font-black mb-1 text-slate-800"><i class="fas fa-tasks text-indigo-500 mr-2"></i>Classroom Application (Slide 32)</h3>')

# Now let's completely replace from app-view-33 downwards until the <script> tag
pattern = r'<!-- Section A: Assessment View \(Slide 33\) -->.*?<script>'

new_content = '''<!-- Section D: Workshop Survey (Slide 33) -->
  <div class="app-view hidden overflow-y-auto pb-20" id="app-view-33">
    <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
      <h3 class="text-2xl font-black mb-1"><i class="fas fa-poll text-indigo-300 mr-2"></i>Workshop Survey</h3>
      <p class="text-sm text-indigo-200">Slide 33: Facilitator Competencies</p>
    </div>
    
    <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-4">
      <p class="text-sm font-semibold text-slate-700 mb-4">Rate the following competencies (4: Excellent, 1: Poor):</p>
      <div class="space-y-4">
        {% set competencies = [
            ('comp_objective', '1. Clear objective'),
            ('comp_sequence', '2. Logical sequence'),
            ('comp_demo', '3. Demonstration'),
            ('comp_participation', '4. Participation'),
            ('comp_guidance', '5. Guidance'),
            ('comp_reading', '6. Reading methodology'),
            ('comp_assessment', '7. Assessment'),
            ('comp_reflection', '8. Reflection')
        ] %}
        
        {% for name, label in competencies %}
        <div class="bg-slate-50 p-3 rounded border border-slate-100 flex items-center justify-between">
            <span class="text-sm font-bold text-slate-800">{{ label }}</span>
            <div class="flex space-x-2">
                <label class="px-2 py-1 bg-white border border-slate-200 rounded text-xs cursor-pointer"><input type="radio" name="{{ name }}" value="4" class="mr-1 ws-radio">4</label>
                <label class="px-2 py-1 bg-white border border-slate-200 rounded text-xs cursor-pointer"><input type="radio" name="{{ name }}" value="3" class="mr-1 ws-radio">3</label>
                <label class="px-2 py-1 bg-white border border-slate-200 rounded text-xs cursor-pointer"><input type="radio" name="{{ name }}" value="2" class="mr-1 ws-radio">2</label>
                <label class="px-2 py-1 bg-white border border-slate-200 rounded text-xs cursor-pointer"><input type="radio" name="{{ name }}" value="1" class="mr-1 ws-radio">1</label>
            </div>
        </div>
        {% endfor %}
      </div>
    </div>
  </div>

  <!-- Section A: Assessment View (Slide 34) -->
  <div class="app-view hidden overflow-y-auto pb-20" id="app-view-34">
    <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
      <h3 class="text-2xl font-black mb-1"><i class="fas fa-award text-yellow-300 mr-2"></i>Final Post-Test</h3>
      <p class="text-sm text-indigo-200">Slide 34: Knowledge MCQ</p>
    </div>
    
    <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-4">
      <div class="mb-4">
        <p class="font-semibold text-sm mb-2 text-slate-700">1. What is the purpose of the LITRE blending-machine concept?</p>
        <div class="space-y-1 text-sm">
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q1" type="radio" value="A"/> A. To replace reading practice</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q1" type="radio" value="B"/> B. To provide a physical and visual representation of the blending process</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q1" type="radio" value="C"/> C. To test handwriting</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q1" type="radio" value="D"/> D. To teach mathematical concepts</label>
        </div>
      </div>
      
      <div class="mb-4">
        <p class="font-semibold text-sm mb-2 text-slate-700">2. What does the palm represent in the LITRE model?</p>
        <div class="space-y-1 text-sm">
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q2" type="radio" value="A"/> A. The classroom</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q2" type="radio" value="B"/> B. The meeting place in the blending model</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q2" type="radio" value="C"/> C. The alphabet</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q2" type="radio" value="D"/> D. The worksheet</label>
        </div>
      </div>

      <div class="mb-4">
        <p class="font-semibold text-sm mb-2 text-slate-700">3. What is the teacher\'s role during guided practice?</p>
        <div class="space-y-1 text-sm">
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q3" type="radio" value="A"/> A. Provide all answers</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q3" type="radio" value="B"/> B. Allow learners to work without guidance</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q3" type="radio" value="C"/> C. Demonstrate, guide, observe and provide corrective support</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q3" type="radio" value="D"/> D. Only mark worksheets</label>
        </div>
      </div>

      <div class="mb-4">
        <p class="font-semibold text-sm mb-2 text-slate-700">4. Why is repetition important in catch-up reading?</p>
        <div class="space-y-1 text-sm">
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q4" type="radio" value="A"/> A. It provides additional opportunities for learners to practise and consolidate skills.</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q4" type="radio" value="B"/> B. It eliminates the need for teaching.</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q4" type="radio" value="C"/> C. It prevents learner participation.</label>
          <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input class="mr-2 mcq-radio" name="q4" type="radio" value="D"/> D. It replaces assessment.</label>
        </div>
      </div>
    </div>
    <button type="submit" class="w-full py-4 bg-green-600 hover:bg-green-700 text-white text-xl font-black rounded-xl shadow-lg transition transform hover:-translate-y-1">
        Submit <i class="fas fa-arrow-right ml-2"></i>
    </button>
  </div>
  </form>
  
  </div>
<script>'''

html = re.sub(pattern, new_content, html, flags=re.DOTALL)

# Need to wrap everything from app-view-31 to app-view-34 in the form!
# We will insert the <form> start tag right before app-view-31
form_start = '<form id="assessment-form" action="{{ url_for(\'sace_bp.submit_post_test\') }}" method="POST"><input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>\n'
html = html.replace('<!-- Section B: Facilitator Evaluation (Slide 31) -->', form_start + '<!-- Section B: Facilitator Evaluation (Slide 31) -->')


# Update simSteps array
old_steps = "{ slide: 29, view: 'p', appView: 33 }"
new_steps = "{ slide: 29, view: 'p', appView: 33 },\n        { slide: 29, view: 'p', appView: 34 }"
html = html.replace(old_steps, new_steps)

# Update applyStep
html = html.replace("else if (step.appView === 33) {\n            document.getElementById('step-counter-container').innerText = \"Post-Test\";\n            nextBtn.style.display = 'none'; // Force them to use the submit button\n        }",
                    "else if (step.appView === 33) {\n            document.getElementById('step-counter-container').innerText = \"Workshop Survey\";\n            nextBtn.style.display = 'block';\n        } else if (step.appView === 34) {\n            document.getElementById('step-counter-container').innerText = \"Post-Test\";\n            nextBtn.style.display = 'none'; // Force them to use the submit button\n        }")

# Update nextStep validation for 33
val_32 = '''// Validation for Slide 32
        if (currentStepData && currentStepData.appView === 32) {
            const checks = document.querySelectorAll('.application-check');
            let allChecked = true;
            checks.forEach(c => {
                if (!c.checked) {
                    allChecked = false;
                    c.closest('label').classList.add('border-red-500', 'bg-red-50');
                }
            });
            
            if (!allChecked) {
                alert("Please tick all the checkboxes to confirm successful inclusion before proceeding.");
                
                setTimeout(() => {
                    document.querySelectorAll('.border-red-500').forEach(el => el.classList.remove('border-red-500', 'bg-red-50'));
                }, 2000);
                
                return; // block advancement
            }
        }'''

val_33 = '''
        // Validation for Slide 33 (Workshop Survey)
        if (currentStepData && currentStepData.appView === 33) {
            const radioGroups = ['comp_objective', 'comp_sequence', 'comp_demo', 'comp_participation', 'comp_guidance', 'comp_reading', 'comp_assessment', 'comp_reflection'];
            let allValid = true;
            radioGroups.forEach(name => {
                if (!document.querySelector(input[name=""]:checked)) {
                    allValid = false;
                }
            });
            if (!allValid) {
                alert("Please complete all ratings in the Workshop Survey.");
                return;
            }
        }'''

html = html.replace(val_32, val_32 + val_33)

# Remove the syncChecks function call entirely since we don't need hidden fields anymore.
# The hidden fields are now gone because we replaced the whole block.
# We also remove onchange="syncChecks(this)" from the html.
html = html.replace('onchange="syncChecks(this)"', '')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(html)
