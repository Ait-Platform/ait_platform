"""Agreed fixed utilities alongside existing PM rent schedules; preserve pm_soa_003."""
from alembic import op
import sqlalchemy as sa

revision='pm_soa_004'
down_revision='pm_soa_003'
branch_labels=None
depends_on=None


def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.add_column('bil_tenant_recurring',sa.Column('pm_charge_type',sa.String(32),nullable=True))
    op.add_column('bil_tenant_recurring',sa.Column('pm_fixed_agreed',sa.Boolean(),nullable=False,server_default=sa.false()))
    op.add_column('bil_tenant_ledger',sa.Column('fixed_charge_type',sa.String(32),nullable=True))
    op.drop_index('uq_pm_recurring_account',table_name='bil_tenant_recurring')
    op.create_index('uq_pm_recurring_account','bil_tenant_recurring',
        ['pm_property_id','tenant_id',sa.text("COALESCE(pm_charge_type,'rent')")],
        unique=True,postgresql_where=sa.text('pm_rent_schedule'))
    op.drop_constraint('ck_pm_recurring_rent','bil_tenant_recurring',type_='check')
    op.create_check_constraint('ck_pm_recurring_rent','bil_tenant_recurring',"""
        NOT pm_rent_schedule OR (
            pm_property_id IS NOT NULL AND pm_manager_id IS NOT NULL
            AND pm_start_date IS NOT NULL AND pm_end_date IS NOT NULL AND pm_end_date>=pm_start_date
            AND pm_version IS NOT NULL AND pm_version>0
            AND pm_create_key IS NOT NULL AND pm_create_hash IS NOT NULL
            AND pm_last_key IS NOT NULL AND pm_last_hash IS NOT NULL
            AND description IS NOT NULL AND description=CASE COALESCE(pm_charge_type,'rent')
                WHEN 'rent' THEN 'Rent Due' WHEN 'water' THEN 'Water' WHEN 'electricity' THEN 'Electricity'
                WHEN 'sewerage' THEN 'Sewerage' WHEN 'refuse' THEN 'Refuse Removal'
                WHEN 'other_utilities' THEN 'Other Utilities' ELSE '' END
            AND COALESCE(pm_charge_type,'rent') IN ('rent','water','electricity','sewerage','refuse','other_utilities')
            AND (COALESCE(pm_charge_type,'rent')='rent' OR pm_fixed_agreed)
            AND kind IS NOT NULL AND kind='charge'
            AND amount IS NOT NULL AND amount>0 AND amount<=99999999.99
            AND day_of_month IS NOT NULL AND day_of_month BETWEEN 1 AND 31
            AND is_active IS NOT NULL AND is_active=0)
    """)
    op.drop_index('uq_pm_ledger_monthly_rent',table_name='bil_tenant_ledger')
    op.create_index('uq_pm_ledger_monthly_rent','bil_tenant_ledger',
        ['tenant_id','rent_property_id',sa.text("COALESCE(fixed_charge_type,'rent')"),'month'],
        unique=True,postgresql_where=sa.text('rent_schedule_id IS NOT NULL'))
    op.drop_constraint('ck_pm_ledger_rent','bil_tenant_ledger',type_='check')
    op.create_check_constraint('ck_pm_ledger_rent','bil_tenant_ledger',"""
        (rent_schedule_id IS NULL AND rent_property_id IS NULL AND fixed_charge_type IS NULL) OR
        (rent_schedule_id IS NOT NULL AND rent_property_id IS NOT NULL
         AND month IS NOT NULL AND month ~ '^[0-9]{4}-[0-9]{2}$'
         AND kind IS NOT NULL AND kind='charge'
         AND description IS NOT NULL AND description=CASE COALESCE(fixed_charge_type,'rent')
            WHEN 'rent' THEN 'Rent Due' WHEN 'water' THEN 'Water' WHEN 'electricity' THEN 'Electricity'
            WHEN 'sewerage' THEN 'Sewerage' WHEN 'refuse' THEN 'Refuse Removal'
            WHEN 'other_utilities' THEN 'Other Utilities' ELSE '' END
         AND COALESCE(fixed_charge_type,'rent') IN ('rent','water','electricity','sewerage','refuse','other_utilities')
         AND txn_date IS NOT NULL AND month=to_char(txn_date,'YYYY-MM')
         AND amount IS NOT NULL AND amount>0)
    """)


def downgrade():
    raise RuntimeError('No destructive downgrade: preserve fixed-charge definitions and posting history.')
