import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove the button entirely
content = re.sub(
    r"\{% elif job_card\.status == 'Awaiting Deposit' %\}.*?Record Deposit\s*</button>\s*\{% endif %\}",
    "",
    content,
    flags=re.DOTALL
)
content = re.sub(
    r"\{% elif job_card\.status not in \['Quote', 'Rejected'\] %\}.*?Record Payment\s*</button>\s*\{% endif %\}",
    "",
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
