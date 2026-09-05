import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Remove the survey GET and POST routes I added earlier
pattern = r'@sace_bp\.route\("/sace/reading/survey".*?(?=@sace_bp\.route|$)'
text = re.sub(pattern, '', text, flags=re.DOTALL)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
