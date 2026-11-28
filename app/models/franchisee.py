"""Registration identity only; never grants roles, entitlements or financial rights."""
from app.extensions import db


class FranchiseeRegistration(db.Model):
    __tablename__ = 'franchise_registration'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(254), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    status = db.Column(db.String(16), nullable=False, default='Invited')
    full_name = db.Column(db.String(160))
    cellphone = db.Column(db.String(20))
    occupation = db.Column(db.String(160))
    identity_ciphertext = db.Column(db.Text)
    address_ciphertext = db.Column(db.Text)
    identity_digest = db.Column(db.String(64))
    invitation_digest = db.Column(db.String(64))
    invitation_expires_at = db.Column(db.DateTime(timezone=True))
    invitation_sent_at = db.Column(db.DateTime(timezone=True))
    created_at = db.Column(db.DateTime(timezone=True), nullable=False)
    submitted_at = db.Column(db.DateTime(timezone=True))
    reviewed_at = db.Column(db.DateTime(timezone=True))
    created_by = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    reviewed_by = db.Column(db.Integer, db.ForeignKey('user.id'))
    franchisee_number = db.Column(db.String(32))
    history = db.Column(db.JSON, nullable=False, default=list)
    __table_args__ = (
        db.UniqueConstraint('email',name='ait_franchisee_registration_email_key'),
        db.UniqueConstraint('user_id',name='ait_franchisee_registration_user_id_key'),
        db.UniqueConstraint('invitation_digest',name='ait_franchisee_registration_invitation_digest_key'),
        db.UniqueConstraint('franchisee_number',name='ait_franchisee_registration_franchisee_number_key'),
        db.CheckConstraint("status IN ('Invited','Submitted','Approved','Rejected')", name='ck_franchisee_status'),
        db.CheckConstraint('email = lower(email)', name='ck_franchisee_email'),
        db.CheckConstraint("(status='Approved') = (franchisee_number IS NOT NULL)", name='ck_franchisee_approved_number'),
        db.CheckConstraint("status='Invited' OR (full_name IS NOT NULL AND cellphone IS NOT NULL AND address_ciphertext IS NOT NULL AND submitted_at IS NOT NULL AND invitation_digest IS NULL)", name='ck_franchisee_submitted'),
    )

    @property
    def is_suspended(self):
        for event in reversed(self.history or []):
            if event.get('action') in {'suspended', 'reinstated'}:
                return event['action'] == 'suspended'
        return False

    @property
    def is_active(self):
        return self.status == 'Approved' and not self.is_suspended
