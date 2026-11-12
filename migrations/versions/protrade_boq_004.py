"""BOQ quantities default to one; unit prices and existing records stay intact."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_boq_004'
down_revision = 'protrade_boq_003'
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
    op.add_column('protrade_boq_items', sa.Column('quantity', sa.Numeric(14, 3), nullable=False, server_default='1'))
    op.create_check_constraint('ck_protrade_boq_quantity', 'protrade_boq_items',
                               'quantity > 0 AND quantity <= 99999999999.999')
    op.create_check_constraint('ck_protrade_boq_each_quantity', 'protrade_boq_items',
                               "unit <> 'Each' OR quantity = trunc(quantity)")
    op.add_column('protrade_boq_items', sa.Column('total_amount', sa.Numeric(28, 2),
                  sa.Computed('quantity * (material_rate + labour_rate)', persisted=True)))


def downgrade():
    raise RuntimeError('Destructive downgrade disabled: preserve BOQ quantities and amounts')
