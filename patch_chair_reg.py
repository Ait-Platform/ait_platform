with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix the occupied_seats query
old_query = """    occupied = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.position).in_(["chairperson", "vice-chairperson", "secretary", "treasurer"])
    ).all()
    
    occupied_seats = [m.position.lower() for m in occupied]"""

new_query = """    occupied = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.position).in_(["chairperson", "chairman", "chair", "vice-chairperson", "vice chairman", "vice chair", "secretary", "treasurer"])
    ).all()
    
    # Normalize the output for the template
    occupied_seats = []
    for m in occupied:
        pos = m.position.lower()
        if pos in ["chairman", "chair"]: pos = "chairperson"
        if pos in ["vice chairman", "vice chair"]: pos = "vice-chairperson"
        occupied_seats.append(pos)"""

if old_query in text:
    text = text.replace(old_query, new_query)
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched occupied_seats query in router_page")
else:
    print("Could not find old query in router_page")

# Fix verify_committee route where it blocks multiple registrations
old_verify = """            singular_roles = ["chairperson", "vice-chairperson", "secretary", "treasurer"]
            if position.lower() in singular_roles:
                occupied = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id,
                    func.lower(UipCommitteeMember.position) == position.lower(),
                    UipCommitteeMember.status == "CURRENT"
                ).first()"""

new_verify = """            norm_pos = position.lower()
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

if old_verify in text:
    text = text.replace(old_verify, new_verify)
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched verify_committee route")
else:
    print("Could not find old verify query")
