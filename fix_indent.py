import re

filepath = 'app/program_uip/secretary_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    if 436 <= i <= 443: # 0-indexed for 437 to 444
        if "if claim.interaction_type" in line:
            new_lines.append("    if claim.interaction_type == \"mo_claim\":\n")
        elif "role_slug = \"municipal_officer\"" in line:
            new_lines.append("        role_slug = \"municipal_officer\"\n")
        elif "elif claim.interaction_type == \"committee_claim\":" in line:
            new_lines.append("    elif claim.interaction_type == \"committee_claim\":\n")
        elif "role_slug = \"committee_member\"" in line:
            new_lines.append("        role_slug = \"committee_member\"\n")
        elif "elif claim.interaction_type == \"ratepayer_claim\":" in line:
            new_lines.append("    elif claim.interaction_type == \"ratepayer_claim\":\n")
        elif "role_slug = \"ratepayer\"" in line:
            new_lines.append("        role_slug = \"ratepayer\"\n")
        elif "else:" in line:
            new_lines.append("    else:\n")
        elif "role_slug = \"subcommittee_member\"" in line:
            new_lines.append("        role_slug = \"subcommittee_member\"\n")
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open(filepath, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
print("Indentation fixed")
