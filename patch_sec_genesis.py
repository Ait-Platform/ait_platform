import sys
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    c = f.read()

# Add seat resolution logic to the genesis flow
search_block = """                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        term_id=term.id,
                        name=claim.creator.name,
                        email=claim.creator.email,
                        position=pos,
                        status="CURRENT"
                    )"""

replace_block = """                    from app.models.uip_governance import UipOrganogramSeat
                    from sqlalchemy import func
                    seat = UipOrganogramSeat.query.filter(
                        UipOrganogramSeat.organization_id == org.id,
                        func.lower(UipOrganogramSeat.title) == func.lower(pos)
                    ).first()
                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        term_id=term.id,
                        name=claim.creator.name,
                        email=claim.creator.email,
                        position=pos,
                        seat_id=seat.id if seat else None,
                        status="CURRENT"
                    )"""

c = c.replace(search_block, replace_block)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Updated secretary_routes.py genesis flow")
