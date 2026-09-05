import re

with open('templates/program_sace/reviewer_guide.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add the strict restart instruction to the Evaluator Mode bullet point
old_bullet = '<li><strong>Evaluator Mode:</strong> The simulator disconnects the dashboard from our live servers. You can now use the "Next" buttons to freely read through all presentation slides, view the live-graphing capabilities, and review the teaching prompts without affecting live sessions.</li>'
new_bullet = '<li><strong>Evaluator Mode:</strong> The simulator disconnects the dashboard from our live servers. You can now use the "Next" buttons to freely read through all presentation slides. <em>Note: To enforce the strict linear flow of the actual app, you cannot go backwards. If you miss something, you must click the Guide (A) tab and restart the simulation from scratch.</em></li>'

text = text.replace(old_bullet, new_bullet)

with open('templates/program_sace/reviewer_guide.html', 'w', encoding='utf-8') as f:
    f.write(text)
