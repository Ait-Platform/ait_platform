"""Reusable BOQ price items; additive, no mechanic catalog changes."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_boq_002'
down_revision = 'protrade_price_001'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('ProTrade BOQ requires PostgreSQL')
    if not op.get_context().as_sql:
        identity = bind.execute(sa.text('SELECT current_database(), current_user')).one()
        if not all(identity):
            raise RuntimeError('Cannot verify migration database identity')
    op.create_table('protrade_boq_items',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('user.id'), nullable=False),
        sa.Column('shop_id', sa.Integer(), sa.ForeignKey('mech_shops.id'), nullable=False),
        sa.Column('trade', sa.String(50), nullable=False),
        sa.Column('code', sa.String(50), nullable=False),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('unit', sa.String(8), nullable=False),
        sa.Column('material_rate', sa.Numeric(14, 2), nullable=False),
        sa.Column('labour_rate', sa.Numeric(14, 2), nullable=False),
        sa.Column('total_rate', sa.Numeric(15, 2), sa.Computed('material_rate + labour_rate', persisted=True)),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint('shop_id', 'trade', 'code', name='uq_protrade_boq_shop_trade_code'),
        sa.CheckConstraint('code = upper(trim(code)) AND length(code) > 0', name='ck_protrade_boq_code'),
        sa.CheckConstraint('length(trim(description)) > 0', name='ck_protrade_boq_description'),
        sa.CheckConstraint("unit IN ('Each', 'm', 'm\u00b2', 'm\u00b3')", name='ck_protrade_boq_unit'),
        sa.CheckConstraint('material_rate BETWEEN 0 AND 999999999999.99 AND labour_rate BETWEEN 0 AND 999999999999.99', name='ck_protrade_boq_rates'))


def downgrade():
    raise RuntimeError('Destructive downgrade disabled: preserve BOQ price data')
