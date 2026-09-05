import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update simSteps
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
        { slide: 29, view: 'f' },
        { slide: 29, view: 'p', appView: 31 },
        { slide: 29, view: 'p', appView: 32 },
        { slide: 29, view: 'p', appView: 33 }
    ];'''
text = re.sub(old_simsteps, new_simsteps, text, flags=re.DOTALL)

# 2. Update applyStep function completely using simple replacement to avoid syntax errors
old_apply = r'function applyStep\(\) \{.*?// Update F Slides'
new_apply = '''function applyStep() {
        const step = simSteps[currentStepIndex];
        
        // Update Counter and Next Button
        let displaySlide = step.slide < 0 ? 0 : step.slide + 1;
        const nextBtn = document.getElementById('global-next-btn');
        
        if (step.appView === 31) {
            document.getElementById('step-counter-container').innerText = "Evaluation of Facilitator";
            nextBtn.style.display = 'block';
        } else if (step.appView === 32) {
            document.getElementById('step-counter-container').innerText = "Evaluation of Participant";
            nextBtn.style.display = 'block';
        } else if (step.appView === 33) {
            document.getElementById('step-counter-container').innerText = "Post-Test";
            nextBtn.style.display = 'none'; // Force them to use the submit button
        } else {
            document.getElementById('step-counter-container').innerHTML = 'Step <span id="f-counter-global">' + displaySlide + '</span> of 30';
            nextBtn.style.display = 'block';
        }

        // Update F Slides'''
text = re.sub(old_apply, new_apply, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
