with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix the incorrect imports
text = text.replace("from app.models.uip_governance import UipResolutionVote", "from app.models.uip import UipResolutionVote")

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Fixed import error")
