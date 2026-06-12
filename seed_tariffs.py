from app import create_app, db
from app.models.billing import BilTariff
from datetime import date

app = create_app()

with app.app_context():
    # Only seed if water tariffs don't exist
    if BilTariff.query.filter_by(utility_type='water').count() == 0:
        tariffs = [
            # Water
            BilTariff(utility_type='water', code='W01', description='0L-200L/day', block_start=0, block_end=200, reduction_factor=0.200, rate=39.39, effective_date=date.today()),
            BilTariff(utility_type='water', code='W02', description='201L-833L/day', block_start=201, block_end=833, reduction_factor=0.633, rate=46.70, effective_date=date.today()),
            BilTariff(utility_type='water', code='W03', description='834L-1KL/day', block_start=834, block_end=1000, reduction_factor=0.167, rate=62.17, effective_date=date.today()),
            BilTariff(utility_type='water', code='W04', description='1KL-1.5KL/day', block_start=1001, block_end=1500, reduction_factor=0.500, rate=95.91, effective_date=date.today()),
            BilTariff(utility_type='water', code='W05', description='>1.5KL/day', block_start=1501, block_end=99999, reduction_factor=999.0, rate=105.39, effective_date=date.today()),
            
            # Sewerage
            BilTariff(utility_type='sewerage', code='S01', description='0L-200L/29Days', block_start=0, block_end=200, reduction_factor=0.200, rate=5.45, effective_date=date.today()),
            BilTariff(utility_type='sewerage', code='S02', description='201L-833L/29Days', block_start=201, block_end=833, reduction_factor=0.633, rate=9.20, effective_date=date.today()),
            BilTariff(utility_type='sewerage', code='S03', description='833L-1KL/29Days', block_start=834, block_end=1000, reduction_factor=0.167, rate=17.54, effective_date=date.today()),
            BilTariff(utility_type='sewerage', code='S04', description='1KL-1.5KL/29Days', block_start=1001, block_end=1500, reduction_factor=999.0, rate=27.38, effective_date=date.today()),
            
            # Fixed Charges
            BilTariff(utility_type='water_fixed', code='W_LOSS', description='Water Loss Charge', block_start=0, block_end=0, reduction_factor=1.0, rate=19.30, effective_date=date.today()),
            BilTariff(utility_type='water_fixed', code='W_MGT', description='Monthly Management Fee', block_start=0, block_end=0, reduction_factor=1.0, rate=144.54, effective_date=date.today()),
            BilTariff(utility_type='sewerage_fixed', code='S_REFUSE', description='Refuse Bins', block_start=0, block_end=0, reduction_factor=1.0, rate=206.93, effective_date=date.today())
        ]
        db.session.bulk_save_objects(tariffs)
        db.session.commit()
        print('Tariffs seeded successfully!')
    else:
        print('Tariffs already exist, skipping seed.')
