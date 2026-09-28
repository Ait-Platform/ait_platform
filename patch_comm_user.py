import sys
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    '''            new_member = UipCommitteeMember(
                term_id=new_term.id,''',
    '''            existing_user = User.query.filter(func.lower(User.email) == func.lower(email)).first()
            new_member = UipCommitteeMember(
                user_id=existing_user.id if existing_user else None,
                term_id=new_term.id,'''
)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(c)
