"""Allow property-bank links without assigning payment purposes. No data rewrite."""
from alembic import op
import sqlalchemy as sa

revision='pm_bank_register_002'
down_revision='pm_lease_payments_001'
branch_labels=None
depends_on=None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    checks={c['name']:c['sqltext'] for c in sa.inspect(bind).get_check_constraints('bil_property_payment_account')}
    expected=checks.get('ck_pm_payment_allocation','')
    normalized=expected.lower().replace('(','').replace(')','')
    if ' '.join(normalized.split())!='for_rental or for_utilities':
        raise RuntimeError('Review unexpected or missing payment allocation constraint before migration')
    op.drop_constraint('ck_pm_payment_allocation','bil_property_payment_account',type_='check')


def downgrade():
    raise RuntimeError('Destructive downgrade disabled: preserve unallocated links and historical allocation flags')
