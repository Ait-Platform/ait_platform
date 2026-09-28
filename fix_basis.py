import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the result_basis assignment
old_basis = '''        # Store live tallies and location in result_basis
        basis = res.result_basis or {}
        basis.update({
            "meeting_location": meeting_location,
            "live_yea": live_yea,
            "live_nay": live_nay,
            "live_abstain": live_abstain,
            "endorsements": [] # Setup for the new safeguard feature
        })
        res.result_basis = basis'''

new_basis = '''        # Store live tallies and location in result_basis
        basis = res.result_basis or {}
        basis["ratification"] = {
            "date": meeting_date_str or str(res.decision_date or db.func.current_date()),
            "location": meeting_location,
            "votes_yea": live_yea,
            "votes_nay": live_nay,
            "votes_abstain": live_abstain,
            "recorded_by_name": current_user.name,
            "recorded_by_email": current_user.email,
            "endorsements": [] # Setup for the new safeguard feature
        }
        res.result_basis = basis'''

content = content.replace(old_basis, new_basis)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed result_basis")
