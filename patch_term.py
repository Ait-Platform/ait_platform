import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """                # GENESIS SECRETARY LOGIC
                if position.lower() == "secretary" and not occupied:
                    new_sec = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=current_user.id,
                        term_id=term.id if term else None,
                        name=current_user.name,
                        email=current_user.email,"""

new_logic = """                # GENESIS SECRETARY LOGIC
                if position.lower() == "secretary" and not occupied:
                    # Auto-create a Genesis term if none exists to prevent IntegrityError
                    if not term:
                        from datetime import datetime
                        term = UipCommitteeTerm(
                            organization_id=org.id,
                            name="Genesis Term",
                            start_date=datetime.utcnow().date(),
                            status="ACTIVE"
                        )
                        db.session.add(term)
                        db.session.flush()

                    new_sec = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=current_user.id,
                        term_id=term.id,
                        name=current_user.name,
                        email=current_user.email,"""

text = text.replace(old_logic, new_logic)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated Genesis Term logic")
