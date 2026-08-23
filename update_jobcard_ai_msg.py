import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    'uploadStatus.textContent = "Error: " + data.error;',
    'uploadStatus.textContent = "AI is currently unavailable due to high traffic. Please enter details manually.";'
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
