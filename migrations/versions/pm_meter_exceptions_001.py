"""Additive account-linked meter incidents, evidence and event history."""
from alembic import op
import sqlalchemy as sa
revision='pm_meter_exceptions_001'
down_revision='pm_utilities_001'
branch_labels=None
depends_on=None

def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.create_table('bil_meter_exception',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('property_id',sa.Integer(),sa.ForeignKey('bil_property.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('account_id',sa.Integer(),sa.ForeignKey('bil_muni_account.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('meter_id',sa.Integer(),sa.ForeignKey('bil_meter.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('exception_type',sa.String(20),nullable=False),sa.Column('incident_date',sa.Date(),nullable=False),
        sa.Column('notification_date',sa.Date()),sa.Column('municipal_reference',sa.String(150),nullable=False,server_default=''),
        sa.Column('description',sa.Text(),nullable=False),sa.Column('remarks',sa.Text(),nullable=False,server_default=''),
        sa.Column('replacement_meter_id',sa.Integer(),sa.ForeignKey('bil_meter.id',ondelete='RESTRICT')),
        sa.Column('replacement_date',sa.Date()),sa.Column('status',sa.String(20),nullable=False,server_default='Open'),
        sa.Column('version',sa.Integer(),nullable=False,server_default='1'),
        sa.Column('submission_key',sa.Uuid(),nullable=False,unique=True),sa.Column('created_by',sa.Integer(),nullable=False),
        sa.Column('created_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.CheckConstraint("exception_type IN ('Stolen','Missing','Removed','Disconnected','Reconnected','Replaced')",name='ck_pm_exception_type'),
        sa.CheckConstraint("status IN ('Open','Reported','Resolved','Closed')",name='ck_pm_exception_status'),
        sa.CheckConstraint('replacement_meter_id IS NULL OR replacement_meter_id<>meter_id',name='ck_pm_exception_not_self'),
        sa.CheckConstraint('(replacement_meter_id IS NULL)=(replacement_date IS NULL)',name='ck_pm_exception_replacement_date'),
        sa.CheckConstraint("exception_type<>'Replaced' OR replacement_meter_id IS NOT NULL",name='ck_pm_exception_replaced'),
        sa.CheckConstraint('notification_date IS NULL OR notification_date>=incident_date',name='ck_pm_exception_notification'),
        sa.CheckConstraint('replacement_date IS NULL OR replacement_date>=incident_date',name='ck_pm_exception_replacement_after'))
    op.create_index('ix_pm_exception_property_account','bil_meter_exception',['property_id','account_id'])
    op.create_table('bil_meter_exception_event',
        sa.Column('id',sa.Integer(),primary_key=True),sa.Column('exception_id',sa.Integer(),sa.ForeignKey('bil_meter_exception.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('kind',sa.String(30),nullable=False),sa.Column('actor_id',sa.Integer(),nullable=False),
        sa.Column('details',sa.JSON(),nullable=False),sa.Column('recorded_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()))
    op.create_table('bil_meter_exception_evidence',
        sa.Column('id',sa.Uuid(),primary_key=True),sa.Column('exception_id',sa.Integer(),sa.ForeignKey('bil_meter_exception.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('filename',sa.String(200),nullable=False),sa.Column('mime_type',sa.String(100),nullable=False),sa.Column('storage_key',sa.Text(),nullable=False,unique=True),
        sa.Column('size_bytes',sa.Integer(),nullable=False),sa.Column('sha256',sa.String(64),nullable=False),sa.Column('created_by',sa.Integer(),nullable=False),
        sa.Column('created_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.UniqueConstraint('exception_id','sha256',name='uq_pm_exception_evidence_content'),
        sa.CheckConstraint('size_bytes>0 AND size_bytes<=10485760',name='ck_pm_exception_evidence_size'))

def downgrade():raise RuntimeError('Forward-only: preserve incidents and evidence history.')
