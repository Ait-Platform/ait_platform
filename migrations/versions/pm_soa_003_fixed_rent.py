"""PM-only monthly rent schedules in the existing recurring table and ledger."""
from alembic import op
import sqlalchemy as sa

revision='pm_soa_003'
down_revision='pm_soa_002'
branch_labels=None
depends_on=None


def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    for column in (
        sa.Column('pm_rent_schedule',sa.Boolean(),nullable=False,server_default=sa.false()),
        sa.Column('pm_active',sa.Boolean(),nullable=False,server_default=sa.false()),
        sa.Column('pm_property_id',sa.Integer(),nullable=True),
        sa.Column('pm_manager_id',sa.Integer(),nullable=True),
        sa.Column('pm_start_date',sa.Date(),nullable=True),
        sa.Column('pm_end_date',sa.Date(),nullable=True),
        sa.Column('pm_version',sa.Integer(),nullable=True),
        sa.Column('pm_create_key',sa.String(32),nullable=True),
        sa.Column('pm_create_hash',sa.String(64),nullable=True),
        sa.Column('pm_last_key',sa.String(32),nullable=True),
        sa.Column('pm_last_hash',sa.String(64),nullable=True)):
        op.add_column('bil_tenant_recurring',column)
    op.create_index('uq_pm_recurring_account','bil_tenant_recurring',
        ['pm_property_id','tenant_id'],unique=True,
        postgresql_where=sa.text('pm_rent_schedule'))
    op.create_index('uq_pm_recurring_create_key','bil_tenant_recurring',['pm_create_key'],unique=True)
    op.create_index('uq_pm_recurring_scope','bil_tenant_recurring',['id','tenant_id','pm_property_id'],unique=True)
    op.create_foreign_key('fk_pm_recurring_property','bil_tenant_recurring','bil_property',
        ['pm_property_id'],['id'],ondelete='RESTRICT')
    op.create_check_constraint('ck_pm_recurring_rent','bil_tenant_recurring',"""
        NOT pm_rent_schedule OR (
            pm_property_id IS NOT NULL AND pm_manager_id IS NOT NULL
            AND pm_start_date IS NOT NULL AND pm_end_date IS NOT NULL AND pm_end_date>=pm_start_date
            AND pm_version IS NOT NULL AND pm_version>0
            AND pm_create_key IS NOT NULL AND pm_create_hash IS NOT NULL
            AND pm_last_key IS NOT NULL AND pm_last_hash IS NOT NULL
            AND description IS NOT NULL AND description='Rent Due'
            AND kind IS NOT NULL AND kind='charge'
            AND amount IS NOT NULL AND amount>0 AND amount<=99999999.99
            AND day_of_month IS NOT NULL AND day_of_month BETWEEN 1 AND 31
            AND is_active IS NOT NULL AND is_active=0)
    """)
    op.add_column('bil_tenant_ledger',sa.Column('rent_schedule_id',sa.Integer(),nullable=True))
    op.add_column('bil_tenant_ledger',sa.Column('rent_property_id',sa.Integer(),nullable=True))
    op.create_foreign_key('fk_pm_ledger_rent_scope','bil_tenant_ledger','bil_tenant_recurring',
        ['rent_schedule_id','tenant_id','rent_property_id'],['id','tenant_id','pm_property_id'],ondelete='RESTRICT')
    op.create_index('uq_pm_ledger_monthly_rent','bil_tenant_ledger',
        ['tenant_id','rent_property_id','month'],unique=True,
        postgresql_where=sa.text('rent_schedule_id IS NOT NULL'))
    op.create_check_constraint('ck_pm_ledger_rent','bil_tenant_ledger',"""
        (rent_schedule_id IS NULL AND rent_property_id IS NULL) OR
        (rent_schedule_id IS NOT NULL AND rent_property_id IS NOT NULL
         AND month IS NOT NULL AND month ~ '^[0-9]{4}-[0-9]{2}$'
         AND kind IS NOT NULL AND kind='charge' AND description IS NOT NULL AND description='Rent Due'
         AND txn_date IS NOT NULL AND month=to_char(txn_date,'YYYY-MM')
         AND amount IS NOT NULL AND amount>0)
    """)


def downgrade():
    raise RuntimeError('No destructive downgrade: retain rent schedules and monthly posting history.')
