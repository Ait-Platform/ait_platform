from app import create_app, db
from app.school_billing.routes import build_electrical_rows, build_water_rows
from app.models.billing import BilTenant

app = create_app()

with app.app_context():
    tenant = BilTenant.query.get(3)
    if tenant:
        print(f"Tenant: {tenant.name}, Sectional Unit ID: {tenant.sectional_unit_id}")
        elec_rows, elec_total = build_electrical_rows(tenant.sectional_unit_id, "2026-05")
        water_meters, water_total = build_water_rows(tenant.sectional_unit_id, "2026-05")
        print("Elec Rows:", elec_rows)
        print("Water Meters:", water_meters)
    else:
        print("Tenant 3 not found.")
