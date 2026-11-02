"""Make municipal source details optional; preserve tariff and audit records."""
from alembic import op
import sqlalchemy as sa
revision = 'pm_tariff_authority_002'
down_revision = 'pm_tariff_authority_001'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    for name, size in (('municipality',200),('municipal_category',200),('source_reference',1000)):
        # DROP NOT NULL is safe even when an operator already made it nullable.
        op.alter_column('bil_tariff_verification', name, existing_type=sa.String(size), nullable=True)


def downgrade():
    raise RuntimeError('Forward-only: preserve optional municipal verification details.')
