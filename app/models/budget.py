# add this file
from app.extensions import db
from datetime import datetime

class BudAccount(db.Model):
    __tablename__ = "bud_account"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    code = db.Column(db.String(32), nullable=False)
    name = db.Column(db.String(120), nullable=False)
    kind = db.Column(db.String(10), nullable=False)  # 'income'|'expense'
    is_active = db.Column(db.Integer, nullable=False, server_default="1")
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP"))

    __table_args__ = (db.UniqueConstraint("user_id", "code", name="uq_bud_account_user_code"),)

class BudLedger(db.Model):
    __tablename__ = "bud_ledger"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    account_id = db.Column(db.Integer, db.ForeignKey("bud_account.id"), nullable=False, index=True)
    txn_date = db.Column(db.Date, nullable=False, index=True)
    description = db.Column(db.String(255), nullable=False, server_default="")
    amount_cents = db.Column(db.Integer, nullable=False)
    source = db.Column(db.String(20), nullable=False, server_default="manual")
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP"))

    account = db.relationship("BudAccount")
