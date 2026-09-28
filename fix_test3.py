import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'seat1 = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Chairperson").first()\n    seat2 = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Secretary").first()',
    'seats = UipOrganogramSeat.query.filter_by(organization_id=data.org.id).all()\n    seat1 = seats[0]\n    seat2 = seats[1]'
)

c = c.replace('position="Chairperson"', 'position=seat1.title')
c = c.replace('position="Secretary"', 'position=seat2.title')
c = c.replace('position="Chairperson", seat_id=chair.seat_id', 'position=chair.position, seat_id=chair.seat_id')
c = c.replace('position="Chairperson", seat_id=None', 'position=chair.position, seat_id=None')

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
