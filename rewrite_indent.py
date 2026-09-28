filepath = 'app/program_uip/secretary_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    if 436 <= i <= 443:
        pass # Skip these lines
    else:
        new_lines.append(line)

# Insert the correct block at 436
correct_block = '''    if claim.interaction_type == "mo_claim":
        role_slug = "municipal_officer"
    elif claim.interaction_type == "committee_claim":
        role_slug = "committee_member"
    elif claim.interaction_type == "ratepayer_claim":
        role_slug = "ratepayer"
    else:
        role_slug = "subcommittee_member"
'''
new_lines.insert(436, correct_block)

with open(filepath, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
print("Lines completely replaced and fixed")
