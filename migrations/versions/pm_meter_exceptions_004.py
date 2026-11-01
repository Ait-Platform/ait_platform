"""Disputed municipal observations are case details, not physical readings."""
from alembic import op
import sqlalchemy as sa
revision='pm_meter_exceptions_004'
down_revision='pm_meter_exceptions_003'
branch_labels=None
depends_on=None

def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.add_column('bil_meter_exception_charge',sa.Column('dispute_details',sa.JSON(),nullable=False,server_default=sa.text("'{}'::json")))
    op.alter_column('bil_meter_exception_charge','amount',existing_type=sa.Numeric(18,2),nullable=True)

def downgrade():raise RuntimeError('Forward-only: preserve disputed municipal observations.')
