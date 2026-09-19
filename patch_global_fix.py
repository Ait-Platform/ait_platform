with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """    # Calculate occupied singular seats so the UI can grey them out
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    occupied = UipCommitteeMember.query.filter("""

new_logic = """    # --- GLOBAL AUTO-FIX FOR ALL CORRUPTED SEATS ---
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    corrupted = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        func.lower(UipCommitteeMember.position).in_(["committee", "unassigned", "committee member", ""])
    ).all()
    if corrupted:
        from app.models.core import CoreInteraction
        for c in corrupted:
            claim = CoreInteraction.query.filter_by(creator_id=c.user_id, interaction_type="committee_claim").first()
            if claim and ":" in claim.title:
                c.position = claim.title.split(": ")[-1].strip()
        db.session.commit()
    # -----------------------------------------------

    # Calculate occupied singular seats so the UI can grey them out
    occupied = UipCommitteeMember.query.filter("""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched global auto-fix in routes.py")
