import esprima
import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

scripts = re.findall(r'<script>(.*?)</script>', text, re.DOTALL)
for i, script in enumerate(scripts):
    # Strip jinja
    script = re.sub(r'\{\{.*?\}\}', '""', script)
    script = re.sub(r'\{%.*?%\}', '', script)
    try:
        esprima.parseScript(script)
        print(f"Script {i+1} is VALID.")
    except Exception as e:
        print(f"Script {i+1} ERROR: {e}")
