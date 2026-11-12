"""Add only missing shop/trade price scope columns; preserve all catalog rows.

Independent branch: upgrade protrade_price_001, never unrelated legacy heads.
Safe after the Stage 3A startup helper or the equivalent manual Render SQL.
"""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_price_001'
down_revision = None
branch_labels = ('protrade_price',)
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('ProTrade price migration requires PostgreSQL')
    if not op.get_context().as_sql:
        identity = bind.execute(sa.text('SELECT current_database(), current_user')).one()
        if not all(identity):
            raise RuntimeError('Cannot verify migration database identity')
        inspector = sa.inspect(bind)
        for table in ('mech_catalog_parts', 'mech_shops'):
            if not inspector.has_table(table):
                raise RuntimeError(f'Required existing table missing: {table}; no DDL applied')
    op.execute('ALTER TABLE mech_catalog_parts ADD COLUMN IF NOT EXISTS shop_id INTEGER REFERENCES mech_shops(id)')
    op.execute('ALTER TABLE mech_catalog_parts ADD COLUMN IF NOT EXISTS trade VARCHAR(50)')


def downgrade():
    # Columns may predate this revision and can contain live shop-specific prices.
    raise RuntimeError('Destructive downgrade disabled: preserve shop/trade price data')
