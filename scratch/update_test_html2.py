import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Hide/skip step 1 on retake, and set step 4 to visible on retake
text = text.replace('<div id="step-1" class="step-container">', '{% if not request.args.get("retake") %}\n            <div id="step-1" class="step-container">')
# Close the if block after step-1's closing tag
# Let's find the Next button for step 1 to locate the end of step 1.
next1_button = '''<button type="button" onclick="nextStep(1, 2)" class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow transition">
                        Next: Evaluating W/S <i class="fas fa-arrow-right ml-2"></i>
                    </button>'''
if next1_button in text:
    replacement = '''<button type="button" onclick="nextStep(1, 4)" class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow transition">
                        Next: Final Assessment <i class="fas fa-arrow-right ml-2"></i>
                    </button>'''
    text = text.replace(next1_button, replacement)
    
    # Insert {% endif %} after step 1
    text = text.replace('<!-- STEP 2: Evaluating W/S -->', '{% endif %}\n\n            <!-- STEP 2: Evaluating W/S -->')

# 2. Make Step 4 visible if retake
step4_start = '<div id="step-4" class="step-container hidden">'
step4_replacement = '<div id="step-4" class="step-container {% if not request.args.get(\'retake\') %}hidden{% endif %}">'
text = text.replace(step4_start, step4_replacement)

# 3. Remove Step 2 and Step 3
# Step 2 starts at <!-- STEP 2: Evaluating W/S --> and ends before <!-- STEP 3: Post-Test (MCQ) --> or <!-- STEP 3: Post-Workshop Survey
# Step 3 starts at <!-- STEP 3 and ends before <div id="step-4"
pattern_2_3 = r'<!-- STEP 2: Evaluating W/S -->.*?<div id="step-4"'
text = re.sub(pattern_2_3, '<div id="step-4"', text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
