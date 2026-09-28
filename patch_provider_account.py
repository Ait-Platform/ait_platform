import sys
with open("app/models/uip.py", "r", encoding="utf-8") as f:
    c = f.read()

model = """class UipProviderAccount(db.Model):
    __tablename__ = "uip_provider_account"
    id = db.Column(db.Integer, primary_key=True)
    provider_id = db.Column(db.Integer, db.ForeignKey("uip_provider.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    is_admin = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    __table_args__ = (
        db.UniqueConstraint("provider_id", "user_id", name="uq_uip_provider_account_user"),
    )

class UipProviderUser(db.Model):"""

c = c.replace("class UipProviderUser(db.Model):", model)

with open("app/models/uip.py", "w", encoding="utf-8") as f:
    f.write(c)
