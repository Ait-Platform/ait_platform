import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
in_accept_quote = False
in_token_block = False

for i, line in enumerate(lines):
    if "def accept_quote(id):" in line:
        in_accept_quote = True
        new_lines.append(line)
        continue
        
    if in_accept_quote:
        if "from app.models.auth import AitTokenWallet, AitTokenTransaction" in line and "    " in line[:8]:
            # This is the start of the bad block. We need to unindent it by 4 spaces.
            in_token_block = True
            
        if in_token_block:
            if line.startswith("        "):
                new_lines.append(line[4:])
            else:
                if "job_card.status = 'Awaiting Deposit'" in line or "job_card = MechJobCard.query.get_or_404(id)" in line:
                    in_token_block = False
                    new_lines.append(line)
                else:
                    new_lines.append(line[4:] if line.startswith("    ") else line)
            continue
            
    new_lines.append(line)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
