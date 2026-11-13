"""Shared password-reset IP limit and registered-account resend cooldown."""
from alembic import op
import sqlalchemy as sa

revision = 'reset_email_security_001'
down_revision = None
branch_labels = ('reset_email_security',)
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('Password-reset email protection requires PostgreSQL')
    if not op.get_context().as_sql:
        if not all(bind.execute(sa.text('SELECT current_database(),current_user')).one()):
            raise RuntimeError('Cannot verify database identity')
    op.create_table('password_reset_request',
        sa.Column('id', sa.String(32), primary_key=True),
        sa.Column('ip_hash', sa.String(64), nullable=False),
        sa.Column('attempted_at', sa.DateTime(timezone=True), nullable=False))
    op.create_index('ix_reset_request_ip_time', 'password_reset_request', ['ip_hash','attempted_at'])
    op.create_table('password_reset_send',
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('user.id'), primary_key=True),
        sa.Column('attempt_id', sa.String(32), nullable=False, unique=True),
        sa.Column('attempted_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('status', sa.String(16), nullable=False),
        sa.CheckConstraint("status IN ('attempted','accepted','uncertain')", name='ck_reset_send_status'))


def downgrade():
    raise RuntimeError('Preserve password-reset sending reservations; destructive downgrade disabled')
