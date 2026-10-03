"""HOME operational subject and isolated endorsement lifecycle. No historical adoption."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
from types import SimpleNamespace
from datetime import datetime, timezone
revision = 'home_sace_002'
down_revision = 'home_sace_001'
branch_labels = None
depends_on = None
Base = declarative_base()
db = SimpleNamespace(**{name: getattr(sa, name) for name in (
    'Column','Integer','String','Boolean','JSON','DateTime','ForeignKey','CheckConstraint','Index','text')})
def now():
    return datetime.now(timezone.utc)
for name in ('user','auth_subject','auth_subject_admin','sace_home_controller',
             'sace_home_provisioning','sace_home_pledge','sace_home_assignment'):
    sa.Table(name, Base.metadata, sa.Column('id', sa.Integer, primary_key=True))

class HomeEngagement(Base):
    __tablename__ = 'sace_home_engagement'
    id = db.Column(db.Integer, primary_key=True)
    reference = db.Column(db.String(80), nullable=False, unique=True)
    status = db.Column(db.String(24), nullable=False, default='active')
    started_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'), nullable=False)
    completion_requested_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'))
    completion_requested_at = db.Column(db.DateTime(timezone=True))
    completion_deadline = db.Column(db.DateTime(timezone=True))
    completed_at = db.Column(db.DateTime(timezone=True))
    revoked_at = db.Column(db.DateTime(timezone=True))
    __table_args__ = (
        db.CheckConstraint("status IN ('active','completion_pending','completed','revoked')", name='ck_home_engagement_status'),
        db.CheckConstraint("(status IN ('active','completion_pending') AND completed_at IS NULL AND revoked_at IS NULL) OR (status='completed' AND completed_at IS NOT NULL AND revoked_at IS NULL) OR (status='revoked' AND revoked_at IS NOT NULL AND completed_at IS NULL)", name='ck_home_engagement_times'),
        db.CheckConstraint("(completion_requested_at IS NULL AND completion_deadline IS NULL AND completion_requested_by_user_id IS NULL AND status != 'completion_pending') OR (completion_requested_at IS NOT NULL AND completion_deadline IS NOT NULL AND completion_requested_by_user_id IS NOT NULL AND completion_deadline = completion_requested_at + interval '48 hours' AND status IN ('completion_pending','completed','revoked'))", name='ck_home_completion_deadline'),
    )


class HomeControllerAppointment(Base):
    __tablename__ = 'sace_home_controller_appointment'
    id = db.Column(db.Integer, primary_key=True)
    controller_id = db.Column(db.Integer, db.ForeignKey('sace_home_controller.id', ondelete='RESTRICT'), nullable=False)
    engagement_id = db.Column(db.Integer, db.ForeignKey('sace_home_engagement.id', ondelete='RESTRICT'), nullable=False)
    operational_grant_id = db.Column(db.Integer, db.ForeignKey('auth_subject_admin.id', ondelete='SET NULL'))
    grant_id_at_issue = db.Column(db.Integer, nullable=False)
    grant_subject_id = db.Column(db.Integer, db.ForeignKey('auth_subject.id', ondelete='RESTRICT'), nullable=False)
    grant_email_at_issue = db.Column(db.String(255), nullable=False)
    provisioning_id = db.Column(db.Integer, db.ForeignKey('sace_home_provisioning.id', ondelete='RESTRICT'), nullable=False, unique=True)
    pledge_id = db.Column(db.Integer, db.ForeignKey('sace_home_pledge.id', ondelete='RESTRICT'), nullable=False, unique=True)
    status = db.Column(db.String(16), nullable=False, default='active')
    started_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)
    ended_at = db.Column(db.DateTime(timezone=True))
    __table_args__ = (
        db.CheckConstraint("status IN ('active','completed','revoked')", name='ck_home_appointment_status'),
        db.CheckConstraint("(status='active' AND operational_grant_id IS NOT NULL AND ended_at IS NULL) OR (status IN ('completed','revoked') AND ended_at IS NOT NULL)", name='ck_home_appointment_state'),
        db.CheckConstraint('operational_grant_id IS NULL OR operational_grant_id = grant_id_at_issue', name='ck_home_exact_grant'),
        db.Index('uq_home_active_controller_appointment', 'controller_id', unique=True, postgresql_where=db.text("status='active'")),
        db.Index('uq_home_active_operational_grant', 'operational_grant_id', unique=True, postgresql_where=db.text("status='active'")),
    )


class HomeAuditEvent(Base):
    __tablename__ = 'sace_home_audit_event'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'), nullable=False)
    engagement_id = db.Column(db.Integer, db.ForeignKey('sace_home_engagement.id', ondelete='RESTRICT'), index=True)
    assignment_id = db.Column(db.Integer, db.ForeignKey('sace_home_assignment.id', ondelete='RESTRICT'))
    role = db.Column(db.String(16), nullable=False)
    event = db.Column(db.String(60), nullable=False, index=True)
    details = db.Column(db.JSON, nullable=False, default=dict)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)

TABLE_NAMES = ('sace_home_engagement','sace_home_controller_appointment','sace_home_audit_event')

def ensure_subject():
    # Unique slug arbitrates duplicates; existing configuration is never overwritten.
    # The existing ck_auth_subject_enroll_policy permits only auto_enroll/post_payment.
    # post_payment prevents automatic enrollment; HOME stays free and R/A authority
    # comes from provisioning/assignment, without payment or checkout.
    op.get_bind().execute(sa.text("""
        INSERT INTO auth_subject
          (slug,name,is_active,trial_days,commercial_mode,billing_scope,enroll_policy,
           processor_default,requires_price,allow_country_pricing,mor_mode,program_type,
           is_hidden_on_bridge,show_on_welcome,start_endpoint,admin_start_endpoint)
        VALUES ('sace_home_endorsement','HOME SACE Endorsement',1,0,'free','user','post_payment',
                'paystack',0,0,0,'free',true,false,'home_sace_bp.entry','home_sace_bp.entry')
        ON CONFLICT (slug) DO NOTHING
    """))

def upgrade():
    ensure_subject()
    for name in TABLE_NAMES:
        Base.metadata.tables[name].create(op.get_bind(), checkfirst=False)
    op.add_column('sace_home_invitation', sa.Column('appointment_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_home_invitation_appointment','sace_home_invitation',
        'sace_home_controller_appointment',['appointment_id'],['id'],ondelete='RESTRICT')

def downgrade():
    for name in TABLE_NAMES:
        if op.get_bind().execute(sa.text('SELECT count(*) FROM '+name)).scalar_one():
            raise RuntimeError('Preserve HOME lifecycle history; use forward recovery.')
    op.drop_constraint('fk_home_invitation_appointment','sace_home_invitation',type_='foreignkey')
    op.drop_column('sace_home_invitation','appointment_id')
    for name in reversed(TABLE_NAMES):
        Base.metadata.tables[name].drop(op.get_bind(),checkfirst=False)
    # Retain subject: it may have pre-existed or acquired unrelated references.
