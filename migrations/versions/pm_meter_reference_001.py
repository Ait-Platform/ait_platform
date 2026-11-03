"""Optional municipal E/W reference, separate from the physical meter number."""
from alembic import op
import sqlalchemy as sa
revision='pm_meter_reference_001'
down_revision='pm_muni_arrangements_001'
branch_labels=None
depends_on=None


def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.add_column('bil_meter',sa.Column('municipal_reference',sa.String(100),nullable=True))


def downgrade():
    raise RuntimeError('Forward-only: preserve municipal meter references.')
