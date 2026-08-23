import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = re.sub(
    r"<form method=\"POST\" action=\"\{\{ url_for\('mechanic_bp\.accept_quote', id=job_card\.id\) \}\}\" class=\"inline m-0\">\s*<input type=\"hidden\" name=\"csrf_token\" value=\"\{\{ csrf_token\(\) \}\}\">\s*<button type=\"submit\".*?Mark as Accepted</button>\s*</form>\s*<button onclick=\"document.getElementById\('reject-quote-modal'\).classList.remove\('hidden'\)\" class=\".*?Mark as Rejected</button>",
    "",
    content
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
