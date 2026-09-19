with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix in the view route
text = text.replace("elif scope in ['EXCO', 'COMMITTEE_ALL']:", "elif scope in ['EXCO', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:")

# Fix in the adopt route
text = text.replace("if scope == 'EXCO':", "if scope in ['EXCO', 'EXCO_CORE', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:")

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee_routes.py quorum scopes")
