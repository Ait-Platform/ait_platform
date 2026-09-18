with open("app/models/uip.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Remove the bad columns from UipMemberProfile
bad_fields = """    # Onboarding Campaign Tracking
    invite_wave = db.Column(db.Integer, nullable=False, default=0)
    last_invite_at = db.Column(db.DateTime, nullable=True)"""
text = text.replace(bad_fields, "")

# 2. Add the new UipMemberCampaign table
new_table = """
class UipMemberCampaign(db.Model):
    __tablename__ = "uip_member_campaign"
    id = db.Column(db.Integer, primary_key=True)
    member_profile_id = db.Column(db.Integer, db.ForeignKey("uip_member_profile.id"), nullable=False, unique=True)
    invite_wave = db.Column(db.Integer, nullable=False, default=0)
    last_invite_at = db.Column(db.DateTime, nullable=True)
    
    # Relationship
    member_profile = db.relationship("UipMemberProfile", backref=db.backref("campaign_status", uselist=False))
"""
text = text + new_table

with open("app/models/uip.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Reverted uip_member_profile and added UipMemberCampaign")
