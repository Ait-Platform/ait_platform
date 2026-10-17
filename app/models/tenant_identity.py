"""Additive reusable PM tenant identity; schema creation is migration-only."""
from app.extensions import db

class BilTenantIdentity(db.Model):
    __tablename__ = 'bil_tenant_identity'
    id = db.Column(db.Integer, primary_key=True)
    manager_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'), nullable=False)
    party_type = db.Column(db.String(32), nullable=False)
    legal_name = db.Column(db.String(200), nullable=False)
    identity_number = db.Column(db.String(80), nullable=False)
    address = db.Column(db.Text, nullable=False)
    email = db.Column(db.String(255), nullable=False, server_default='')
    phone = db.Column(db.String(50), nullable=False, server_default='')
    signatory_name = db.Column(db.String(200), nullable=False, server_default='')
    signatory_identity_number = db.Column(db.String(80), nullable=False, server_default='')
    signatory_capacity = db.Column(db.String(200), nullable=False, server_default='')
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    __table_args__ = (
        db.CheckConstraint("party_type IN ('natural_person','juristic_person')", name='ck_pm_tenant_type'),
        db.CheckConstraint("length(trim(legal_name)) > 0 AND length(trim(identity_number)) > 0 AND length(trim(address)) > 0", name='ck_pm_tenant_required'),
        db.CheckConstraint("party_type = 'natural_person' OR (length(trim(signatory_name)) > 0 AND length(trim(signatory_identity_number)) > 0 AND length(trim(signatory_capacity)) > 0)", name='ck_pm_tenant_representative'),
        db.Index('uq_pm_tenant_identity', 'manager_id', 'party_type', db.func.lower(db.func.replace(identity_number, ' ', '')), unique=True),
    )
