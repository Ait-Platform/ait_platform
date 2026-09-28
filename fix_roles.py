import re

filepath = 'app/program_uip/secretary_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

bad_role = 'role_slug = "municipal_officer" if claim.interaction_type == "mo_claim" else "subcommittee_member"'

good_role = '''if claim.interaction_type == "mo_claim":
          role_slug = "municipal_officer"
      elif claim.interaction_type == "committee_claim":
          role_slug = "committee_member"
      elif claim.interaction_type == "ratepayer_claim":
          role_slug = "ratepayer"
      else:
          role_slug = "subcommittee_member"'''

content = content.replace(bad_role, good_role)

bad_position = 'position="Municipal Officer" if claim.interaction_type == "mo_claim" else "Verified Member",'
good_position = '''position="Municipal Officer" if claim.interaction_type == "mo_claim" else ("Committee Member" if claim.interaction_type == "committee_claim" else "Ratepayer"),'''

content = content.replace(bad_position, good_position)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Route roles fixed")
