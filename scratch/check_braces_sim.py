import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

scripts = re.findall(r'<script>(.*?)</script>', text, re.DOTALL)
for i, script in enumerate(scripts):
    open_braces = script.count('{')
    close_braces = script.count('}')
    print(f"Script {i+1}: Open braces: {open_braces}, Close braces: {close_braces}")
