import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# We need to change the redirect at the end of accept_quote.
# Find def accept_quote(id): and its return statement.
regex = r'def accept_quote\(id\):.*?return redirect\(request\.referrer or url_for\(\'mechanic_bp\.job_cards_list\'\)\)'

def replacer(match):
    return match.group(0).replace("return redirect(request.referrer or url_for('mechanic_bp.job_cards_list'))", "return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))")

new_content = re.sub(regex, replacer, content, flags=re.DOTALL)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(new_content)
