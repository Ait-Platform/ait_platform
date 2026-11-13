"""Durable cooldown and delivery state for two legacy LOSS email actions."""
from alembic import op
import sqlalchemy as sa

revision = 'loss_email_security_001'
down_revision = None
branch_labels = ('loss_email_security',)
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('LOSS email protection requires PostgreSQL')
    if not op.get_context().as_sql:
        if not all(bind.execute(sa.text('SELECT current_database(),current_user')).one()):
            raise RuntimeError('Cannot verify database identity')
    op.create_table('loss_report_email_send',
        sa.Column('run_id', sa.Integer(), sa.ForeignKey('lca_run.id'), primary_key=True),
        sa.Column('owner_id', sa.Integer(), sa.ForeignKey('user.id'), nullable=False),
        sa.Column('actor_id', sa.Integer(), sa.ForeignKey('user.id'), nullable=False),
        sa.Column('recipient', sa.Text(), nullable=False),
        sa.Column('attempt_id', sa.String(32), nullable=False, unique=True),
        sa.Column('status', sa.String(16), nullable=False),
        sa.Column('attempted_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("status IN ('attempted','accepted','uncertain')", name='ck_loss_email_status'))
    op.create_index('ix_loss_email_owner_attempt', 'loss_report_email_send', ['owner_id','attempted_at'])


def downgrade():
    raise RuntimeError('Preserve LOSS email delivery protection records; destructive downgrade disabled')
