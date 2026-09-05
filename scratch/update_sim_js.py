import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

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

old_counter_js = r"let displaySlide = step\.slide < 0 \? 0 : step\.slide \+ 1;\s*document\.getElementById\('f-counter-global'\)\.innerText = displaySlide;"
new_counter_js = '''let displaySlide = step.slide < 0 ? 0 : step.slide + 1;
        if (step.appView === 9 || step.appView === 10 || step.slide === 28 || step.slide === 29) {
            document.getElementById('step-counter-container').innerText = "Post-Test";
        } else {
            document.getElementById('step-counter-container').innerHTML = Step <span id="f-counter-global"></span> of 30;
        }'''

text = re.sub(old_counter_js, new_counter_js, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
