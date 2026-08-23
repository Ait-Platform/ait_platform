import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Let's see what the table header looks like
print(re.search(r'<thead.*?</thead>', content, re.DOTALL).group(0))

