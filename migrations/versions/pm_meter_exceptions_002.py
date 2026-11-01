"""Continuing municipal charges linked to incident and captured bill; no posting."""
from alembic import op
import sqlalchemy as sa
revision='pm_meter_exceptions_002'
down_revision='pm_meter_exceptions_001'
branch_labels=None
depends_on=None

def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.create_table('bil_meter_exception_charge',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('exception_id',sa.Integer(),sa.ForeignKey('bil_meter_exception.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('statement_id',sa.Integer(),sa.ForeignKey('bil_municipal_statement.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('amount',sa.Numeric(18,2),nullable=False),sa.Column('charge_reference',sa.String(150),nullable=False,server_default=''),
        sa.Column('description',sa.Text(),nullable=False),sa.Column('remarks',sa.Text(),nullable=False,server_default=''),
        sa.Column('bill_snapshot',sa.JSON(),nullable=False),sa.Column('submission_key',sa.Uuid(),nullable=False,unique=True),
        sa.Column('created_by',sa.Integer(),nullable=False),sa.Column('created_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.CheckConstraint('amount>0',name='ck_pm_exception_charge_positive'))
    op.create_index('ix_pm_exception_charge_case','bil_meter_exception_charge',['exception_id'])
    op.create_table('bil_meter_exception_charge_evidence',
        sa.Column('charge_id',sa.Integer(),sa.ForeignKey('bil_meter_exception_charge.id',ondelete='RESTRICT'),primary_key=True),
        sa.Column('evidence_id',sa.Uuid(),sa.ForeignKey('bil_meter_exception_evidence.id',ondelete='RESTRICT'),primary_key=True))

def downgrade():raise RuntimeError('Forward-only: preserve continuing charge history.')
