import re

file_path = 'AGENT.md'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

pattern = r'than "SACE\'s Evaluation Hub"\)\. \(Critical Domain Context\).*?Evaluation Hub"\)\.'
text = re.sub(pattern, 'than "SACE\\\'s Evaluation Hub").', text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
