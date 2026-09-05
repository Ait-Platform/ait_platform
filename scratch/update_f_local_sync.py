import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Make updateView post the message if parent exists
old_update = """    function updateView() {"""
new_update = """    function updateView() {
        if (window.parent && window.parent !== window) { 
            window.parent.postMessage({action: 'slideChanged', slide: currentSlide, state: sessionState}, '*'); 
        }"""
text = text.replace(old_update, new_update)

# Fix the cb param in previous links just in case
text = text.replace('cb=1788072626', 'cb={{ range(1, 999999) | random }}')

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
