"""Private BOQ preparation time, independent of client/financial registers."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_boq_time_016'
down_revision = 'protrade_work_activity_015'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name != 'postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run BOQ preparation migration online on PostgreSQL')
    op.create_table('protrade_boq_preparation',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('user.id'), nullable=False),
        sa.Column('shop_id', sa.Integer(), sa.ForeignKey('mech_shops.id'), nullable=False),
        sa.Column('debtor_id', sa.Integer(), sa.ForeignKey('debtor.id'), nullable=False),
        sa.Column('quote_key', sa.String(32), nullable=False),
        sa.Column('quote_id', sa.Integer(), sa.ForeignKey('protrade_boq_quotes.id')),
        sa.Column('started_at', sa.DateTime(), nullable=False),
        sa.Column('completed_at', sa.DateTime()),
        sa.Column('total_seconds', sa.Numeric(16, 3), nullable=False, server_default='0'),
        sa.Column('last_seen_at', sa.DateTime()),
        sa.Column('tab_key', sa.String(32)),
        sa.Column('tick', sa.Integer(), nullable=False, server_default='0'),
        sa.UniqueConstraint('user_id', 'quote_key', name='uq_boq_preparation_key'),
        sa.UniqueConstraint('user_id', 'quote_id', name='uq_boq_preparation_quote'),
        sa.CheckConstraint('total_seconds >= 0 AND tick >= 0', name='ck_boq_preparation_time'))


def downgrade():
    raise RuntimeError('Preserve BOQ preparation history')
