import re

with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Make sure to query ALL claim types (including committee_claim and unknown_claim)
old_query = """        CoreInteraction.interaction_type.in_([
            "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim"
        ])"""

new_query = """        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])"""

text = text.replace(old_query, new_query)

# Add title and description to enriched_claims
old_enrich = """        enriched_claims.append({
            "id": claim.id,
            "type": claim.interaction_type,
            "created_at": claim.created_at,
            "user_name": creator.name if creator else "Unknown",
            "user_email": creator.email if creator else "Unknown",
        })"""

new_enrich = """        enriched_claims.append({
            "id": claim.id,
            "type": claim.interaction_type,
            "title": claim.title,
            "description": claim.description,
            "created_at": claim.created_at,
            "user_name": creator.name if creator else "Unknown",
            "user_email": creator.email if creator else "Unknown",
        })"""

text = text.replace(old_enrich, new_enrich)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated secretary_routes.py")

