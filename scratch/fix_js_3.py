import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Just replace the entire if/else block
old_block_pattern = r'if \(step\.appView === 9.*?\} else \{.*?\}'
new_block = '''if (step.appView === 9 || step.appView === 10 || step.slide === 28 || step.slide === 29) {
            document.getElementById('step-counter-container').innerText = "Post-Test";
        } else {
            document.getElementById('step-counter-container').innerHTML = Step <span id="f-counter-global"></span> of 30;
        }'''

text = re.sub(old_block_pattern, new_block, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

