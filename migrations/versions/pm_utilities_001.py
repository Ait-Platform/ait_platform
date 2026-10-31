"""Independent utility capture and append-only correction audit."""
from alembic import op
import sqlalchemy as sa
revision='pm_utilities_001'
down_revision='metsoa_setup_002'
branch_labels=None
depends_on=None

def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.create_table('bil_municipal_statement',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('account_id',sa.Integer(),sa.ForeignKey('bil_muni_account.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('period',sa.String(7),nullable=False),sa.Column('statement_date',sa.Date(),nullable=False),
        sa.Column('arrears',sa.Numeric(18,2),nullable=False),sa.Column('balance',sa.Numeric(18,2),nullable=False),sa.Column('due',sa.Numeric(18,2),nullable=False),
        sa.Column('due_date',sa.Date(),nullable=False),sa.Column('version',sa.Integer(),nullable=False,server_default='1'),
        sa.UniqueConstraint('account_id','period',name='uq_municipal_statement_period'),
        sa.CheckConstraint("period=to_char(statement_date,'YYYY-MM')",name='ck_municipal_statement_period'))
    op.create_table('bil_utilities_capture_audit',
        sa.Column('id',sa.Integer(),primary_key=True),sa.Column('property_id',sa.Integer(),sa.ForeignKey('bil_property.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('kind',sa.String(20),nullable=False),sa.Column('record_id',sa.Integer(),nullable=False),
        sa.Column('actor_id',sa.Integer(),nullable=False),sa.Column('before_data',sa.JSON(),nullable=True),sa.Column('after_data',sa.JSON(),nullable=False),
        sa.Column('recorded_at',sa.DateTime(timezone=True),server_default=sa.func.now(),nullable=False))

def downgrade():raise RuntimeError('Forward-only: preserve captured statements and correction history.')
