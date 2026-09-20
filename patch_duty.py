with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """                # Grant the appropriate role
                role_slug = "committee_member" if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"] else "mo" if "mo" in claim.interaction_type else "resident"
                role_obj = CoreRole.query.filter_by(slug=role_slug).first()"""

new_logic = """                # Grant the appropriate role
                role_slug = "committee_member" if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"] else "mo" if "mo" in claim.interaction_type else "owner"
                
                # Check for custom Duty from Organogram Seat
                if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"]:
                    pos = "Secretary" if claim.interaction_type == "secretary_claim" else (claim.title.split(": ")[-1] if ":" in claim.title else (claim.title.split(" - ")[-1] if " - " in claim.title else claim.title))
                    from app.models.uip_governance import UipOrganogramSeat
                    seat_record = UipOrganogramSeat.query.filter(UipOrganogramSeat.organization_id == org.id, func.lower(UipOrganogramSeat.title) == func.lower(pos.strip())).first()
                    if seat_record and seat_record.duty:
                        role_slug = seat_record.duty
                
                role_obj = CoreRole.query.filter_by(slug=role_slug).first()"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched role assignment based on duty")
