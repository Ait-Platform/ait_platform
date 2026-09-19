with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """        if corrupted:
            from app.models.core import CoreInteraction
            for c in corrupted:
                claim = CoreInteraction.query.filter_by(creator_id=c.user_id, interaction_type="committee_claim").first()
                if claim and ":" in claim.title:
                    c.position = claim.title.split(": ")[-1].strip()
            db.session.commit()"""

new_logic = """        if corrupted:
            from app.models.core import CoreInteraction
            for c in corrupted:
                claim = CoreInteraction.query.filter_by(creator_id=c.user_id, interaction_type="committee_claim").first()
                if claim:
                    if ":" in claim.title:
                        c.position = claim.title.split(": ")[-1].strip()
                    elif " - " in claim.title:
                        c.position = claim.title.split(" - ")[-1].strip()
            db.session.commit()"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched routes.py to handle hyphen splitting in title")
