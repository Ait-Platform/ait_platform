"""Add missing METSOA wizard columns without assigning or changing existing records.
Target this independent revision only. No application startup is invoked.
"""
from alembic import op
import sqlalchemy as sa
revision = 'metsoa_setup_001'
down_revision = None
branch_labels = ('metsoa_setup',)
depends_on = None

COLUMNS = {
    'bil_muni_account': [
        ('property_id', sa.Integer()),
        ('is_bulk_account', sa.Boolean()),
        ('muni_email', sa.String(255)),
        ('owner_address', sa.String(500)),
        ('water_meter_id', sa.Integer()),
        ('elec_meter_id', sa.Integer()),
        ('muni_water_meter_no', sa.String(50)),
        ('muni_water_ref', sa.String(50)),
        ('muni_elec_meter_no', sa.String(50)),
        ('muni_elec_ref', sa.String(50)),
        ('rates_reference', sa.String(100)),
        ('rates_erf_details', sa.String(255)),
        ('rates_property_category', sa.String(100)),
        ('rates_market_value', sa.Float()),
        ('rates_rateable_value', sa.Float()),
        ('rates_general_randage', sa.Float()),
        ('rates_sra_randage', sa.Float()),
        ('rates_deferred', sa.Float()),
        ('rates_sra_monthly', sa.Float()),
        ('rates_general_monthly', sa.Float()),
        ('valuation', sa.Float()),
        ('arrears_amount', sa.Float()),
        ('arrears_date', sa.Date()),
        ('arrears_charge_to', sa.String(20)),
        ('rates_amount', sa.Float()),
        ('rates_date', sa.Date()),
        ('rates_charge_to', sa.String(20)),
        ('arrangement_date', sa.Date()),
        ('arrangement_charge_to', sa.String(20)),
        ('ca_contract_number', sa.String(100)),
        ('ca_agreement_amount', sa.Float()),
        ('ca_installments_raised', sa.Float()),
        ('ca_installment_amount', sa.Float()),
        ('ca_amount_owing', sa.Float()),
        ('ca_remaining_periods', sa.Integer()),
    ],
    'bil_meter': [
        ('parent_meter_id', sa.Integer()),
        ('replacement_for_meter_id', sa.Integer()),
        ('pointing_to', sa.String(100)),
        ('municipal_bill_number', sa.String(100)),
        ('status', sa.String(50)),
        ('date_stolen', sa.Date()),
        ('date_replaced', sa.Date()),
        ('property_id', sa.Integer()),
    ],
}
REFERENCES = {
    ('bil_muni_account','property_id'): ('bil_property','id','RESTRICT'),
    ('bil_muni_account','water_meter_id'): ('bil_meter','id','SET NULL'),
    ('bil_muni_account','elec_meter_id'): ('bil_meter','id','SET NULL'),
    ('bil_meter','property_id'): ('bil_property','id','RESTRICT'),
    ('bil_meter','parent_meter_id'): ('bil_meter','id','RESTRICT'),
    ('bil_meter','replacement_for_meter_id'): ('bil_meter','id','RESTRICT'),
}

def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('PostgreSQL required')
    bind.execute(sa.text("SET LOCAL lock_timeout = '10s'"))
    catalog = sa.inspect(bind)
    required = {
        'bil_property': {'id'},
        'bil_muni_account': {'id','account_number','owner_id'},
        'bil_meter': {'id','meter_number','utility_type'},
        'ref_muni_owner': {'id','name'},
        'bil_meter_reading': {'id','meter_id','reading_date','reading_value'},
        'bil_consumption': {'id','meter_id'},
        'bil_architecture_draft': {'id','property_id','draft_json','updated_at'},
    }
    for table, columns in required.items():
        if not catalog.has_table(table) or not columns <= {c['name'] for c in catalog.get_columns(table)}:
            raise RuntimeError('METSOA prerequisite schema missing: '+table+'; inspect before migrating. No records were changed.')
    bind.execute(sa.text('LOCK TABLE bil_muni_account, bil_meter IN ACCESS EXCLUSIVE MODE'))
    added = []
    for table, columns in COLUMNS.items():
        existing = {c['name'] for c in sa.inspect(bind).get_columns(table)}
        for name, kind in columns:
            if name not in existing:
                op.add_column(table, sa.Column(name,kind,nullable=True))
                added.append((table,name))
    for table,name in added:
        target = REFERENCES.get((table,name))
        if target:
            remote,key,ondelete = target
            op.create_foreign_key('fk_metsoa_setup_'+table+'_'+name,table,remote,[name],[key],ondelete=ondelete)
    # No UPDATE, DELETE, default values or association backfill.

def downgrade():
    raise RuntimeError('Forward-only: dropping these columns could lose verified associations. Restore code without removing data.')
