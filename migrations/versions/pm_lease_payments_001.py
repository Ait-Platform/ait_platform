"""Property lease payment links and versioned utility arrangements only.

No production execution is authorized. Existing bank/property/municipal records
and all existing Alembic heads remain unchanged on this independent branch.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB
revision='pm_lease_payments_001'
down_revision=None
branch_labels=('pm_lease_payments',)
depends_on=None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    inspector=sa.inspect(bind)
    for name in ('bil_property_payment_account','bil_property_utility_arrangement'):
        if inspector.has_table(name):raise RuntimeError('Existing payment/utility objects require independent review')
    for name,columns in {'bil_property':{'id','manager_id'},'user':{'id'},'bil_bank_detail':{'id','bank_name','account_holder','account_number','account_type','branch_code'}}.items():
        if not inspector.has_table(name) or not columns<={c['name'] for c in inspector.get_columns(name)}:
            raise RuntimeError('Existing property/user/bank foundation required')
    op.create_table('bil_property_payment_account',
        sa.Column('id',UUID(as_uuid=True),primary_key=True),
        sa.Column('property_id',sa.Integer,sa.ForeignKey('bil_property.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('bank_detail_id',sa.Integer,sa.ForeignKey('bil_bank_detail.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('for_rental',sa.Boolean,nullable=False),sa.Column('for_utilities',sa.Boolean,nullable=False),
        sa.Column('created_by',sa.Integer,sa.ForeignKey('user.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('created_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.UniqueConstraint('property_id','bank_detail_id',name='uq_pm_property_payment_bank'),
        sa.CheckConstraint('for_rental OR for_utilities',name='ck_pm_payment_allocation'))
    op.create_table('bil_property_utility_arrangement',
        sa.Column('id',UUID(as_uuid=True),primary_key=True),
        sa.Column('property_id',sa.Integer,sa.ForeignKey('bil_property.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('version',sa.Integer,nullable=False),sa.Column('details',JSONB,nullable=False),
        sa.Column('created_by',sa.Integer,sa.ForeignKey('user.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('created_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.UniqueConstraint('property_id','version',name='uq_pm_property_utility_version'),
        sa.CheckConstraint('version>0',name='ck_pm_property_utility_version'),
        sa.CheckConstraint("jsonb_typeof(details)='object'",name='ck_pm_property_utility_details'))


def downgrade():
    raise RuntimeError('Destructive downgrade disabled: preserve bank links and utility history; roll back application code instead')
