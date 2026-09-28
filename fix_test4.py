import sys
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'seats = UipOrganogramSeat.query.filter_by(organization_id=data.org.id).all()\n    seat1 = seats[0]\n    seat2 = seats[1]',
    'seat1 = UipOrganogramSeat(organization_id=data.org.id, title="Chairperson")\n    seat2 = UipOrganogramSeat(organization_id=data.org.id, title="Secretary")\n    db.session.add_all([seat1, seat2])\n    db.session.flush()'
)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
