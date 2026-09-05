import re

with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the href for the Participant App button
content = content.replace("href=\"{{ url_for('sace.interactive_workshop') }}\"", "href=\"{{ url_for('sace.participant_join') }}\"")

with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
    f.write(content)
