"""Registration identity only; never grants roles, entitlements or financial rights."""
from app.extensions import db


class FranchiseeRegistration(db.Model):
    __tablename__ = 'ait_franchisee_registration'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(254), nullable=False, unique=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), unique=True)
    status = db.Column(db.String(16), nullable=False, default='Invited')
    full_name = db.Column(db.String(160))
    cellphone = db.Column(db.String(20))
    occupation = db.Column(db.String(160))
    identity_ciphertext = db.Column(db.Text)
    address_ciphertext = db.Column(db.Text)
    identity_digest = db.Column(db.String(64), unique=True)
    invitation_digest = db.Column(db.String(64), unique=True)
    invitation_expires_at = db.Column(db.DateTime(timezone=True))
    invitation_sent_at = db.Column(db.DateTime(timezone=True))
    created_at = db.Column(db.DateTime(timezone=True), nullable=False)
    submitted_at = db.Column(db.DateTime(timezone=True))
    reviewed_at = db.Column(db.DateTime(timezone=True))
    created_by = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    reviewed_by = db.Column(db.Integer, db.ForeignKey('user.id'))
    franchisee_number = db.Column(db.String(32), unique=True)
    history = db.Column(db.JSON, nullable=False, default=list)
    __table_args__ = (
        db.CheckConstraint("status IN ('Invited','Submitted','Approved','Rejected')", name='ck_franchisee_status'),
        db.CheckConstraint('email = lower(email)', name='ck_franchisee_email'),
        db.CheckConstraint("(status='Approved') = (franchisee_number IS NOT NULL)", name='ck_franchisee_approved_number'),
        db.CheckConstraint("status='Invited' OR (full_name IS NOT NULL AND cellphone IS NOT NULL AND identity_ciphertext IS NOT NULL AND address_ciphertext IS NOT NULL AND identity_digest IS NOT NULL AND submitted_at IS NOT NULL AND invitation_digest IS NULL)", name='ck_franchisee_submitted'),
    )
