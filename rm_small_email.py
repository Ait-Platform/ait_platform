import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove the small paper airplane icon for quotes
content = re.sub(
    r"<a href=\"\{\{ url_for\('mechanic_bp\.email_document', id=job_card\.id\) \}\}\" class=\"w-10 h-10 rounded-full bg-indigo-50.*?</a\>",
    "",
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
