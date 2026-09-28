import sys
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    c = f.read()

search_block1 = """                mem = UipCommitteeMember(
                    organization_id=org.id,
                    term_id=term.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=port,
                    status="CURRENT"
                )"""

replace_block1 = """                from app.models.uip_governance import UipOrganogramSeat
                from sqlalchemy import func
                seat = UipOrganogramSeat.query.filter(
                    UipOrganogramSeat.organization_id == org.id,
                    func.lower(UipOrganogramSeat.title) == func.lower(port)
                ).first()
                mem = UipCommitteeMember(
                    organization_id=org.id,
                    term_id=term.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=port,
                    seat_id=seat.id if seat else None,
                    status="CURRENT"
                )"""

c = c.replace(search_block1, replace_block1)

search_block2 = """                mem = UipCommitteeMember(
                    organization_id=org.id,
                    user_id=claim.creator.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=port,
                    status="CURRENT"
                )"""

replace_block2 = """                from app.models.uip_governance import UipOrganogramSeat
                from sqlalchemy import func
                seat = UipOrganogramSeat.query.filter(
                    UipOrganogramSeat.organization_id == org.id,
                    func.lower(UipOrganogramSeat.title) == func.lower(port)
                ).first()
                mem = UipCommitteeMember(
                    organization_id=org.id,
                    user_id=claim.creator.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=port,
                    seat_id=seat.id if seat else None,
                    status="CURRENT"
                )"""

c = c.replace(search_block2, replace_block2)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Updated committee_routes.py genesis flow")
