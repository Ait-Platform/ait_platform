with open("app/program_uip/services/documents.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

new_accessible = """def accessible(org, actor, document):
    try:
        audit.authorize(org, actor, VISIBILITY.get(document.access_classification, ("manager",)))
        return True
    except Forbidden:
        # Narrow exception for Municipal Officer to view RP_QUERY_PHOTO on referred tickets
        if document.category == "RP_QUERY_PHOTO" and document.interaction_id:
            from app.models.core import CoreRoleAssignment, CoreRole
            is_mo = CoreRoleAssignment.query.join(CoreRole).filter(
                CoreRoleAssignment.organization_id == org,
                CoreRoleAssignment.user_id == actor,
                CoreRole.slug == "municipal_officer"
            ).first()
            if is_mo:
                from app.models.uip import UipMunicipalReferral
                referral = UipMunicipalReferral.query.filter_by(
                    organization_id=org, interaction_id=document.interaction_id
                ).first()
                if referral:
                    return True
        return False"""

pattern = re.compile(r'def accessible\(org, actor, document\):.*?return False', re.DOTALL)
text = pattern.sub(new_accessible, text)

with open("app/program_uip/services/documents.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated accessible")
