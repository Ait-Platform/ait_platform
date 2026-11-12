"""Carpentry quote snapshots and explicit mechanic shop links; no invoice changes."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_quote_005'
down_revision = 'protrade_boq_004'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('ProTrade quotes require PostgreSQL')
    if not op.get_context().as_sql:
        identity = bind.execute(sa.text('SELECT current_database(), current_user')).one()
        if not all(identity):
            raise RuntimeError('Cannot verify migration database identity')
    op.execute('ALTER TABLE mech_job_cards ADD COLUMN IF NOT EXISTS shop_id INTEGER REFERENCES mech_shops(id)')
    op.create_table('protrade_boq_quotes',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('user.id'), nullable=False),
        sa.Column('shop_id', sa.Integer(), sa.ForeignKey('mech_shops.id'), nullable=False),
        sa.Column('client_id', sa.Integer(), sa.ForeignKey('mech_clients.id'), nullable=True),
        sa.Column('trade', sa.String(50), nullable=False),
        sa.Column('quote_number', sa.String(50), nullable=False, unique=True),
        sa.Column('client_name', sa.String(150), nullable=False),
        sa.Column('client_phone', sa.String(50)), sa.Column('client_email', sa.String(150)),
        sa.Column('client_address', sa.Text()), sa.Column('items', sa.JSON(), nullable=False),
        sa.Column('grand_total', sa.Numeric(28, 2), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.CheckConstraint("trade = 'carpentry'", name='ck_protrade_boq_quote_trade'),
        sa.CheckConstraint('grand_total >= 0', name='ck_protrade_boq_quote_total'))


def downgrade():
    raise RuntimeError('Destructive downgrade disabled: preserve existing quotation data')
