"""Add isolated Admin-approved franchisee registrations; preserve all existing users."""
from alembic import op
import sqlalchemy as sa

revision = 'ait_franchisee_001'
down_revision = 'protrade_trade_codes_019'
branch_labels = None
depends_on = None


def upgrade():
    context = op.get_context()
    config = getattr(context, 'config', None)
    approved = context.opts.get('franchisee_registration_approved') is True or (
        config is not None and config.attributes.get('franchisee_registration_approved') is True)
    if not approved:
        raise RuntimeError('Franchisee migration requires an explicitly approved Alembic context; startup auto-upgrade is not authorised')
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('Franchisee registration requires PostgreSQL')
    op.execute('CREATE SEQUENCE ait_franchisee_number_seq START WITH 1')
    op.create_table('ait_franchisee_registration',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('email', sa.String(254), nullable=False, unique=True),
        sa.Column('user_id', sa.Integer, sa.ForeignKey('user.id'), unique=True),
        sa.Column('status', sa.String(16), nullable=False),
        sa.Column('full_name', sa.String(160)), sa.Column('cellphone', sa.String(20)),
        sa.Column('occupation', sa.String(160)),
        sa.Column('identity_ciphertext', sa.Text), sa.Column('address_ciphertext', sa.Text),
        sa.Column('identity_digest', sa.String(64), unique=True),
        sa.Column('invitation_digest', sa.String(64), unique=True),
        sa.Column('invitation_expires_at', sa.DateTime(timezone=True)),
        sa.Column('invitation_sent_at', sa.DateTime(timezone=True)),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('submitted_at', sa.DateTime(timezone=True)),
        sa.Column('reviewed_at', sa.DateTime(timezone=True)),
        sa.Column('created_by', sa.Integer, sa.ForeignKey('user.id'), nullable=False),
        sa.Column('reviewed_by', sa.Integer, sa.ForeignKey('user.id')),
        sa.Column('franchisee_number', sa.String(32), unique=True),
        sa.Column('history', sa.JSON, nullable=False),
        sa.CheckConstraint("status IN ('Invited','Submitted','Approved','Rejected')", name='ck_franchisee_status'),
        sa.CheckConstraint('email = lower(email)', name='ck_franchisee_email'),
        sa.CheckConstraint("(status='Approved') = (franchisee_number IS NOT NULL)", name='ck_franchisee_approved_number'),
        sa.CheckConstraint("status='Invited' OR (full_name IS NOT NULL AND cellphone IS NOT NULL AND identity_ciphertext IS NOT NULL AND address_ciphertext IS NOT NULL AND identity_digest IS NOT NULL AND submitted_at IS NOT NULL AND invitation_digest IS NULL)", name='ck_franchisee_submitted'),
    )


def downgrade():
    raise RuntimeError('Retain franchisee identities, approval history and issued numbers; use a reviewed rollback')
