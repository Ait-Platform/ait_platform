with open("app/models/uip_governance.py", "r", encoding="utf-8") as f:
    text = f.read()

# Add UipOrganogramSeat
new_model = """
class UipOrganogramSeat(db.Model):
    __tablename__ = "uip_organogram_seat"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    title = db.Column(db.String(100), nullable=False)
    group_level = db.Column(db.String(50), nullable=False) # CORE_EXCO, SECOND_GROUP
    qualifier = db.Column(db.String(50), nullable=False, default="Voluntary")
    display_order = db.Column(db.Integer, default=0)

"""

if "UipOrganogramSeat" not in text:
    text = new_model + text

# Add columns to UipCommitteeMember
if "photo_url = db.Column(" not in text:
    old_member = """    updated_at = db.Column(db.DateTime(timezone=True), onupdate=db.func.now())"""
    new_member = """    updated_at = db.Column(db.DateTime(timezone=True), onupdate=db.func.now())
    seat_id = db.Column(db.Integer, db.ForeignKey("uip_organogram_seat.id"), nullable=True)
    photo_url = db.Column(db.String(500), nullable=True)"""
    text = text.replace(old_member, new_member)

with open("app/models/uip_governance.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated models in uip_governance.py")
