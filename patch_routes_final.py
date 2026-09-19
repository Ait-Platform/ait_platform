import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

pattern = re.compile(r'if corrupted:\s*from app.models.core import CoreInteraction\s*for c in corrupted:\s*claim = CoreInteraction.query.filter_by\(creator_id=c.user_id, interaction_type="committee_claim"\).first\(\)\s*if claim and ":" in claim.title:\s*c.position = claim.title.split\(": "\)\[-1\].strip\(\)\s*db.session.commit\(\)', re.MULTILINE | re.DOTALL)

new_logic = """if corrupted:
        from app.models.core import CoreInteraction
        from app.models.auth import User
        from sqlalchemy import func
        for c in corrupted:
            user = User.query.filter(func.lower(User.email) == func.lower(c.email)).first()
            if user:
                claim = CoreInteraction.query.filter_by(creator_id=user.id, interaction_type="committee_claim").first()
                if claim:
                    if ":" in claim.title:
                        c.position = claim.title.split(": ")[-1].strip()
                    elif " - " in claim.title:
                        c.position = claim.title.split(" - ")[-1].strip()
        db.session.commit()"""

text, count = pattern.subn(new_logic, text)
print(f"Replacements made: {count}")

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
