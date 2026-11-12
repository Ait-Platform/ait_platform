"""Shop-owned ProTrade debtors and reusable draft identity. No legacy backfill."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_debtor_006'
down_revision = 'protrade_quote_005'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('ProTrade debtors require PostgreSQL')
    if not op.get_context().as_sql:
        if not all(bind.execute(sa.text('SELECT current_database(), current_user')).one()):
            raise RuntimeError('Cannot verify database identity')
    op.execute('ALTER TABLE debtor ADD COLUMN IF NOT EXISTS shop_id INTEGER REFERENCES mech_shops(id)')
    op.execute('ALTER TABLE debtor ADD COLUMN IF NOT EXISTS address TEXT')
    op.execute('ALTER TABLE mech_clients ADD COLUMN IF NOT EXISTS shop_id INTEGER REFERENCES mech_shops(id)')
    op.execute('ALTER TABLE protrade_boq_quotes ADD COLUMN IF NOT EXISTS debtor_id INTEGER REFERENCES debtor(id)')
    op.execute('ALTER TABLE protrade_boq_quotes ADD COLUMN IF NOT EXISTS is_draft BOOLEAN')
    op.execute('ALTER TABLE mech_job_cards ADD COLUMN IF NOT EXISTS debtor_id INTEGER REFERENCES debtor(id)')
    op.execute('ALTER TABLE mech_job_cards ADD COLUMN IF NOT EXISTS is_draft BOOLEAN')
    op.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_protrade_boq_open_draft ON protrade_boq_quotes (shop_id,debtor_id) WHERE is_draft IS TRUE AND debtor_id IS NOT NULL')
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_protrade_mech_open_draft ON mech_job_cards (shop_id,debtor_id) WHERE status = 'Quote' AND is_draft IS TRUE AND debtor_id IS NOT NULL")



def downgrade():
    raise RuntimeError('Destructive downgrade disabled: preserve debtor ownership and quotation links')
