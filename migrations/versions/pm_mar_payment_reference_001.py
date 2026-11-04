"""Add a MAR payment reference sequence without rewriting payment records."""
from alembic import op
import sqlalchemy as sa
revision='pm_mar_payment_reference_001'
down_revision='pm_meter_reference_001'
branch_labels=None
depends_on=None


def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.execute(sa.schema.CreateSequence(sa.Sequence('bil_mar_payment_reference_seq',start=1,increment=1,cycle=False)))
    # Old account-number/bank references are retained. Start after existing 3-digit references.
    op.execute("""SELECT setval('bil_mar_payment_reference_seq',
        COALESCE(MAX(CAST(reference AS integer)),0)+1,false)
        FROM bil_muni_payment WHERE reference ~ '^[0-9]{3}$'""")


def downgrade():
    raise RuntimeError('Forward-only: preserve allocated payment reference identity.')
