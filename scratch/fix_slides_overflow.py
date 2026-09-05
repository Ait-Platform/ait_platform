import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Revert to Dark Background
text = text.replace('<div id="tab-f" class="w-full h-full hidden flex-col relative bg-slate-100">', 
                    '<div id="tab-f" class="w-full h-full hidden flex-col relative bg-slate-900">')

# 2. Add overflow-y-auto to all slide-containers
text = text.replace('class="slide-container absolute inset-0 hidden flex-col', 'class="slide-container absolute inset-0 hidden flex-col overflow-y-auto')
text = text.replace('class="slide-container absolute inset-0 flex flex-col', 'class="slide-container absolute inset-0 flex flex-col overflow-y-auto')
# Also target any slides that might not have flex-col explicitly
text = text.replace('class="slide-container absolute inset-0 hidden items-center', 'class="slide-container absolute inset-0 hidden overflow-y-auto items-center')

# 3. Update the 1ReadingState.png path
text = text.replace("filename='sace_slides/1ReadingState.png'", "filename='uploads/1ReadingState.png'")

# 4. Enforce a safer max height on images to ensure they don't blow out the container
# Find all img tags in the slides and ensure they don't exceed container
text = text.replace('class="max-h-full max-w-full object-contain"', 'class="max-h-[60vh] max-w-full object-contain mx-auto"')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
