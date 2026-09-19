import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace(
    """requested_pos = claim.title.split(": ")[-1] if ":" in claim.title else claim.interaction_type.replace('_claim', '').title()""",
    """requested_pos = claim.title.split(": ")[-1] if ":" in claim.title else (claim.title.split(" - ")[-1] if " - " in claim.title else claim.interaction_type.replace('_claim', '').title())"""
)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
