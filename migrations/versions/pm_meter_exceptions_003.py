"""Exception-only historical identities; never activate or reassign physical meters."""
from alembic import op
import sqlalchemy as sa
revision='pm_meter_exceptions_003'
down_revision='pm_meter_exceptions_002'
branch_labels=None
depends_on=None

def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.create_table('bil_meter_exception_identity',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('property_id',sa.Integer(),sa.ForeignKey('bil_property.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('account_id',sa.Integer(),sa.ForeignKey('bil_muni_account.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('meter_number',sa.String(100),nullable=False),
        sa.Column('identity_key',sa.String(100),nullable=False,unique=True),
        sa.Column('utility_type',sa.String(20),nullable=False),
        sa.Column('verification_basis',sa.Text(),nullable=False),
        sa.Column('created_by',sa.Integer(),nullable=False),
        sa.Column('created_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.CheckConstraint("utility_type IN ('Water','Electrical')",name='ck_pm_exception_identity_utility'),
        sa.CheckConstraint("length(btrim(verification_basis))>0",name='ck_pm_exception_identity_basis'))
    op.add_column('bil_meter_exception',sa.Column('historical_identity_id',sa.Integer(),sa.ForeignKey('bil_meter_exception_identity.id',ondelete='RESTRICT')))
    op.alter_column('bil_meter_exception','meter_id',existing_type=sa.Integer(),nullable=True)
    op.create_check_constraint('ck_pm_exception_one_identity','bil_meter_exception','(meter_id IS NULL) <> (historical_identity_id IS NULL)')

def downgrade():raise RuntimeError('Forward-only: preserve historical exception identities.')
