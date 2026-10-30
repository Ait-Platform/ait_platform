"""Permit property-based municipal meters; retain all legacy unit associations."""
from alembic import op
import sqlalchemy as sa
revision = 'metsoa_setup_002'
down_revision = 'metsoa_setup_001'
branch_labels = None
depends_on = None

def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('PostgreSQL required')
    bind.execute(sa.text("SET LOCAL lock_timeout = '10s'"))
    columns = {c['name']: c for c in sa.inspect(bind).get_columns('bil_meter')}
    if not {'id', 'property_id'} <= columns.keys():
        raise RuntimeError('Meter prerequisite schema missing; inspect before migrating.')
    # Some installations never had this legacy column. Do not create it there.
    if 'sectional_unit_id' in columns and not columns['sectional_unit_id']['nullable']:
        op.alter_column('bil_meter', 'sectional_unit_id', nullable=True)

def downgrade():
    raise RuntimeError('Forward-only: restoring NOT NULL would reject valid property-based meters.')
