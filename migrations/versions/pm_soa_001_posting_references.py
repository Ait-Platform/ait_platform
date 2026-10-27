"""PM ledger references, idempotency metadata and one opening balance per tenant.

Existing financial rows are never updated. Target this revision independently.
"""
from alembic import op
import sqlalchemy as sa

revision = 'pm_soa_001'
down_revision = None
branch_labels = ('pm_soa',)
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('PostgreSQL required')
    # Prevent an older worker inserting a conflicting reference during preparation.
    bind.execute(sa.text('LOCK TABLE bil_tenant_ledger IN ACCESS EXCLUSIVE MODE'))
    if bind.execute(sa.text("""SELECT EXISTS(SELECT tenant_id FROM bil_tenant_ledger
        WHERE kind='opening_balance' OR lower(btrim(description))='opening balance'
        GROUP BY tenant_id HAVING count(*)>1)""")).scalar():
        raise RuntimeError('Duplicate historical opening balances require review; no rows were changed.')
    if bind.execute(sa.text("""SELECT EXISTS(SELECT ref FROM bil_tenant_ledger
        WHERE ref ~ '^[DC][0-9]{4}$' GROUP BY ref HAVING count(*)>1)""")).scalar():
        raise RuntimeError('Duplicate historical D/C references require review; no rows were changed.')
    op.add_column('bil_tenant_ledger', sa.Column('posting_key', sa.String(32), nullable=True))
    op.add_column('bil_tenant_ledger', sa.Column('posting_hash', sa.String(64), nullable=True))
    op.add_column('bil_tenant_ledger', sa.Column('posting_notes', sa.Text(), nullable=True))
    op.create_index('uq_pm_ledger_posting_key', 'bil_tenant_ledger', ['posting_key'], unique=True)
    op.create_index('uq_pm_ledger_opening', 'bil_tenant_ledger', ['tenant_id'], unique=True,
        postgresql_where=sa.text("kind='opening_balance' OR lower(btrim(description))='opening balance'"))
    op.create_index('uq_pm_ledger_dc_ref', 'bil_tenant_ledger', ['ref'], unique=True,
        postgresql_where=sa.text("ref ~ '^[DC][0-9]{4}$'"))
    for prefix, name in (('D','pm_ledger_debit_ref_seq'), ('C','pm_ledger_credit_ref_seq')):
        bind.execute(sa.text('CREATE SEQUENCE '+name+' MINVALUE 1 MAXVALUE 9999 NO CYCLE'))
        maximum = bind.execute(sa.text("""SELECT max(substring(ref from 2)::integer)
            FROM bil_tenant_ledger WHERE ref ~ :pattern"""), {'pattern':'^'+prefix+'[0-9]{4}$'}).scalar()
        if maximum:
            bind.execute(sa.text("SELECT setval(CAST(:name AS regclass), :maximum, true)"),
                dict(name=name, maximum=maximum))


def downgrade():
    raise RuntimeError('No destructive automatic downgrade: retain permanent references and posting safeguards.')
