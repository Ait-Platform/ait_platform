import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Grab everything between "function applyStep() {" and "// Update F Slides"
pattern = r'function applyStep\(\) \{.*?// Update F Slides'

new_apply_step = '''function applyStep() {
        const step = simSteps[currentStepIndex];
        
        // Update Counter
        let displaySlide = step.slide < 0 ? 0 : step.slide + 1;
        if (step.appView === 9 || step.appView === 10) {
            document.getElementById('step-counter-container').innerText = "Post-Test";
        } else {
            document.getElementById('step-counter-container').innerHTML = Step <span id="f-counter-global"></span> of 30;
        }

        // Update F Slides'''

text = re.sub(pattern, new_apply_step, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

