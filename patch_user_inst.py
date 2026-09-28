import os
import re

files = [
    "app/program_uip/committee_routes.py",
    "app/program_uip/provisioning_routes.py",
    "app/program_uip/routes.py",
    "app/program_uip/secretary_routes.py"
]

for file in files:
    if not os.path.exists(file):
        continue
    with open(file, "r", encoding="utf-8") as f:
        content = f.read()
        
    # Example 1: `user_id=claim.creator.id` where claim is used.
    # committee_routes.py: 105 `email=claim.creator.email,` -> add `user_id=claim.creator.id,`
    content = re.sub(
        r'(email=claim\.creator\.email,\n\s*position=port,)',
        r'user_id=claim.creator.id,\n                        \1',
        content
    )
    
    # committee_routes.py: 444 `email=email,` -> `user_id=current_user.id` ? wait!
    # "new_member = UipCommitteeMember(... created_by=current_user.id)"
    # Is there a user_id for this? It's adding a member by email. We should look up the user by email!
    
    # secretary_routes.py: 248
    # mem = UipCommitteeMember( ... email=claim.creator.email ... )
    content = re.sub(
        r'(name=claim\.creator\.name,\n\s*email=claim\.creator\.email,)',
        r'user_id=claim.creator.id,\n                          \1',
        content
    )
    
    # routes.py: 487
    # new_sec = UipCommitteeMember( ... email=current_user.email ... )
    content = re.sub(
        r'(name=current_user\.name,\n\s*email=current_user\.email,)',
        r'user_id=current_user.id,\n                        \1',
        content
    )
    
    with open(file, "w", encoding="utf-8") as f:
        f.write(content)
        
print("Replacements done")
