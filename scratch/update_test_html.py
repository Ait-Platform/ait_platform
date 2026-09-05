import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Remove Step 2
pattern_step2 = r'<!-- STEP 2: Evaluating W/S -->.*?</div>\s*</div>\s*</div>'
# Wait, regex for removing exactly the div step-2 is tricky because of nested divs.
