import re

with open("app/models/uip.py", "r", encoding="utf-8") as f:
    text = f.read()

# Modify UipResolution
old_fields = """    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    status = db.Column(db.String(50), default="PROPOSED") # PROPOSED, APPROVED, REJECTED, EXECUTED"""

new_fields = """    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    status = db.Column(db.String(50), default="PROPOSED") # PROPOSED, APPROVED, REJECTED, EXECUTED
    
    voting_scope = db.Column(db.String(20), default="EXCO") # EXCO, PUBLIC
    quorum_target = db.Column(db.Integer, default=50) # Percentage (e.g. 50%)
    expires_at = db.Column(db.DateTime, nullable=True)"""

text = text.replace(old_fields, new_fields)

# Add UipResolutionVote and UipResolutionComment
new_models = """

class UipResolutionVote(db.Model):
    __tablename__ = "uip_resolution_vote"
    id = db.Column(db.Integer, primary_key=True)
    resolution_id = db.Column(db.Integer, db.ForeignKey("uip_resolution.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    vote = db.Column(db.String(20), nullable=False) # YEA, NAY, ABSTAIN
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint("resolution_id", "user_id", name="uq_uip_resolution_vote"),
    )
    
    voter = db.relationship("User", backref="resolution_votes")
    resolution = db.relationship("UipResolution", backref=db.backref("votes", lazy="dynamic"))


class UipResolutionComment(db.Model):
    __tablename__ = "uip_resolution_comment"
    id = db.Column(db.Integer, primary_key=True)
    resolution_id = db.Column(db.Integer, db.ForeignKey("uip_resolution.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    message = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    
    author = db.relationship("User", backref="resolution_comments")
    resolution = db.relationship("UipResolution", backref=db.backref("comments", lazy="dynamic", order_by="UipResolutionComment.timestamp.asc()"))

"""

if new_models not in text:
    text += new_models

with open("app/models/uip.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated models")
