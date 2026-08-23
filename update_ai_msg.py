import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace:
# uploadStatus.textContent = "AI Error: " + data.error;
# With:
# uploadStatus.textContent = "AI is currently unavailable due to high demand. Please enter details manually.";

content = content.replace(
    'uploadStatus.textContent = "AI Error: " + data.error;',
    'uploadStatus.textContent = "AI is currently unavailable due to high traffic. Please enter details manually.";'
)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
