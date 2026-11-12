"""Allow Hour for predefined carpentry work; preserve BOQ rows and rates."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_boq_003'
down_revision = 'protrade_boq_002'
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
    op.drop_constraint('ck_protrade_boq_unit', 'protrade_boq_items', type_='check')
    op.create_check_constraint('ck_protrade_boq_unit', 'protrade_boq_items',
                               "unit IN ('Each', 'm', 'm\u00b2', 'm\u00b3', 'Hour')")


def downgrade():
    raise RuntimeError('Destructive downgrade disabled: preserve Hour BOQ items')
