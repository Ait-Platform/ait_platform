import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

bad_mem = """                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=claim.creator.id,
                        name=claim.creator.name,
                        email=claim.creator.email,
                        position=pos,
                        status="CURRENT"
                    )"""

good_mem = """                    from app.models.uip_governance import UipCommitteeTerm
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
                        position=pos,
                        status="CURRENT"
                    )"""

if bad_mem in text:
    text = text.replace(bad_mem, good_mem)
    with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched finalize_access_resolution")
else:
    print("Could not find bad_mem in secretary_routes.py")
