import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# The duplicate block starts right after </table> and ends before {% if shop and shop.terms_and_conditions %}
regex = r'(\s*</table>\s*)(.*?)(?=\{% if shop and shop\.terms_and_conditions %\})'

def replacer(match):
    # Only remove it if it contains "Bank Details"
    if "Bank Details" in match.group(2):
        return match.group(1) # Keep just the table and whitespace
    return match.group(0)

content = re.sub(regex, replacer, content, flags=re.DOTALL)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
