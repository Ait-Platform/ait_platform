import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Fix the validation bypass on step 1 and step 2
text = text.replace('onclick="goToStep(2)"', 'onclick="nextStep(1, 2)"')
text = text.replace('onclick="goToStep(3)"', 'onclick="nextStep(2, 3)"')

# Change title of Evaluating F
text = text.replace('Evaluating F', 'Evaluation of Facilitator(s) Presentation')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
