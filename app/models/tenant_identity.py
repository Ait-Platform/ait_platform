"""Representative of a registered bil_tenant; legacy columns retained for compatibility."""
from app.extensions import db

class BilTenantIdentity(db.Model):
    __tablename__ = 'bil_tenant_identity'
    id = db.Column(db.Integer, primary_key=True)
    manager_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'), nullable=False)
    tenant_id = db.Column(db.Integer, db.ForeignKey('bil_tenant.id', ondelete='RESTRICT'), nullable=False)
    party_type = db.Column(db.String(32), nullable=True)
    legal_name = db.Column(db.String(200), nullable=False)
    identity_number = db.Column(db.String(80), nullable=False)
    address = db.Column(db.Text, nullable=True)
    email = db.Column(db.String(255), nullable=False, server_default='')
    phone = db.Column(db.String(50), nullable=False, server_default='')
    signatory_name = db.Column(db.String(200), nullable=True, server_default='')
    signatory_identity_number = db.Column(db.String(80), nullable=True, server_default='')
    signatory_capacity = db.Column(db.String(200), nullable=True, server_default='')
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    __table_args__ = (
        db.UniqueConstraint('tenant_id', name='uq_pm_tenant_representative'),
        db.CheckConstraint("length(trim(legal_name)) > 0 AND length(trim(identity_number)) > 0 AND length(trim(email)) > 0 AND length(trim(phone)) > 0", name='ck_pm_representative_required'),
    )
