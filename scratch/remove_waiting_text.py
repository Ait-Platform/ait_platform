import re
file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Remove the text "Waiting for activity to begin..."
text = text.replace('<p class="text-slate-600 mb-6">Waiting for activity to begin...</p>', '')

# 2. Remove the { slide: -1, view: 'f' } step from simSteps
text = text.replace("{ slide: -1, view: 'f' },\n", "")

# 3. Clean up any references to slide-lobby in JS, just to be safe
text = text.replace("step.slide === -1 ? 'slide-lobby' : 'slide-' + step.slide;", "'slide-' + step.slide;")

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
