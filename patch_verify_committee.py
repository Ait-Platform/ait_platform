with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """            # Rule 2: Seat Occupied Fallback
            # Only check for specific singular roles
            norm_pos = position.lower()
            if norm_pos in ["chairman", "chair"]: norm_pos = "chairperson"
            if norm_pos in ["vice chairman", "vice chair"]: norm_pos = "vice-chairperson"
            
            singular_roles = ["chairperson", "vice-chairperson", "secretary", "treasurer"]
            if norm_pos in singular_roles:
                # Check for any variation of the role
                variations = [norm_pos]
                if norm_pos == "chairperson": variations = ["chairperson", "chairman", "chair"]
                if norm_pos == "vice-chairperson": variations = ["vice-chairperson", "vice chairman", "vice chair"]
                
                occupied = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id,
                    func.lower(UipCommitteeMember.position).in_(variations),
                    UipCommitteeMember.status == "CURRENT"
                ).first()"""

new_logic = """            # Rule 2: Seat Occupied Fallback
            # Dynamically block if seat is already occupied by someone
            norm_pos = position.lower()
            if norm_pos in ["chairman", "chair"]: norm_pos = "chairperson"
            if norm_pos in ["vice chairman", "vice chair"]: norm_pos = "vice-chairperson"
            
            variations = [norm_pos, position.lower()]
            if norm_pos == "chairperson": variations.extend(["chairperson", "chairman", "chair"])
            if norm_pos == "vice-chairperson": variations.extend(["vice-chairperson", "vice chairman", "vice chair"])
            
            occupied = UipCommitteeMember.query.filter(
                UipCommitteeMember.organization_id == org.id,
                func.lower(UipCommitteeMember.position).in_(variations),
                UipCommitteeMember.status == "CURRENT"
            ).first()
            
            if True:  # Changed structure slightly to keep indent matching
                pass"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched verify_committee to dynamically check all occupied seats")
