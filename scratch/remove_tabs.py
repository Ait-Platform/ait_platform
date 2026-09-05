import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Remove the F and P tab buttons from the top bar
text = re.sub(r'<button onclick="switchTab\(\'f\'\)".*?</button>', '', text, flags=re.DOTALL)
text = re.sub(r'<button onclick="switchTab\(\'p\'\)".*?</button>', '', text, flags=re.DOTALL)

# Remove the F and P tab content (iframes)
text = re.sub(r'<!-- Tab F: Facilitator Board -->.*?</div>', '', text, flags=re.DOTALL)
text = re.sub(r'<!-- Tab P: Participant Board -->.*?</div>', '', text, flags=re.DOTALL)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
