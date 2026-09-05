import re

with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    text = f.read()

# We need to find the Second Row block and remove it.
start_marker = "<!-- Second Row (2 Columns) -->"
end_marker = "<!-- Third Row (Report) -->"

if start_marker in text and end_marker in text:
    old_block = text[text.find(start_marker):text.find(end_marker)]
    text = text.replace(old_block, "\n    ")
    
    with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Removed the separate F and P tiles")
else:
    print("Could not find the Second Row markers")
