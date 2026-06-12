from app import create_app, db
from app.models.billing import BilTenant

app = create_app()
with app.app_context():
    t = BilTenant.query.get(3)
    if t and t.leases:
        print(f"Start date: {t.leases[0].start_date}")
        print(f"Day of month: {t.leases[0].day_of_month}")
    else:
        print("No lease found.")
