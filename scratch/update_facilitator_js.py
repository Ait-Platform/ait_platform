import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add postMessage to slide transition functions
def replace_func(func_name, code_to_insert):
    global text
    pattern = r'(function ' + func_name + r'\s*\([^)]*\)\s*\{)'
    text = re.sub(pattern, r'\1\n    ' + code_to_insert, text)

# For goToSlide
replace_func('goToSlide', "if (window.parent && window.parent !== window) { window.parent.postMessage({action: 'switchToParticipant'}, '*'); }")
# For nextSlide
replace_func('nextSlide', "if (window.parent && window.parent !== window) { window.parent.postMessage({action: 'switchToParticipant'}, '*'); }")
# For prevSlide
replace_func('prevSlide', "if (window.parent && window.parent !== window) { window.parent.postMessage({action: 'switchToParticipant'}, '*'); }")
# For startWorkshop
replace_func('startWorkshop', "if (window.parent && window.parent !== window) { window.parent.postMessage({action: 'switchToParticipant'}, '*'); }")


with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated facilitator_dashboard.html with postMessage")
