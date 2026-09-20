with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We need to replace UipOrganization with CoreOrganization
old_function_imports = """    from app.models.uip import UipOrganization
    from app.models.uip_governance import UipOrganogramSeat, UipCommitteeMember
    
    org = UipOrganization.query.filter_by(slug=org_slug).first_or_404()"""

new_function_imports = """    from app.models.core import CoreOrganization
    from app.models.uip_governance import UipOrganogramSeat, UipCommitteeMember
    
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()"""

text = text.replace(old_function_imports, new_function_imports)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Fixed CoreOrganization import")
