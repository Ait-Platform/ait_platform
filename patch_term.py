import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix UipCommitteeMember creation in execute_resolution_adoption
old_mem = """                mem = UipCommitteeMember(
                    organization_id=org.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=port,
                    status="CURRENT"
                )"""

new_mem = """                # Get or create a term
                from app.models.uip_governance import UipCommitteeTerm
                term = UipCommitteeTerm.query.filter_by(organization_id=org.id).first()
                if not term:
                    term = UipCommitteeTerm(organization_id=org.id, term_name="Genesis Term")
                    db.session.add(term)
                    db.session.flush()
                
                mem = UipCommitteeMember(
                    organization_id=org.id,
                    term_id=term.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=port,
                    status="CURRENT"
                )"""

if old_mem in text:
    text = text.replace(old_mem, new_mem)
    with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched helper with term_id")
else:
    print("Could not find mem block")
