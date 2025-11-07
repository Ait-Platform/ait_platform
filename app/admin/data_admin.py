# app/admin/data_admin.py
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
import traceback

# One Admin instance, distinct from your admin_bp blueprint
admin_ui = Admin(
    name="Billing Data",
    template_mode="bootstrap4",
    endpoint="billing_admin",        # changed from data_admin
    url="/admin/billing-data",       # changed from /admin/data
)

def init_admin(app):
    """Call from create_app() AFTER db.init_app(app) and after models are imported."""
    # Debug: show existing blueprints so you can spot duplicates
    print("BLUEPRINTS at init_admin:", sorted(app.blueprints.keys()))

    from app.extensions import db
    from app.models.billing import (
        BilLease,
        BilMeterFixedCharge,
        BilTenant,
        BilMeter,
        BilMeterReading,
        BilSectionalUnit,
    )

    admin_ui.init_app(app)

    class BaseView(ModelView):
        can_view_details = True
        column_display_pk = True
        page_size = 50
        category = "Billing Data"

    class UnitView(BaseView):
        column_list = ("id", "name")
        column_searchable_list = ("name",)

    class TenantView(BaseView):
        column_list = (
            "id", "name", "sectional_unit",
            "metro_account_no", "rent_includes_metro",
            "email", "phone",
        )
        column_searchable_list = ("name", "email", "phone", "metro_account_no")

    class MeterView(BaseView):
        column_list = ("id", "meter_number", "utility_type", "sectional_unit")
        column_searchable_list = ("meter_number", "utility_type")

    class LeaseView(BaseView):
        column_list = (
            "id", "tenant", "sectional_unit",
            "lease_start", "lease_end", "rent_includes_metro"
        )

    def add(view, *, endpoint):
        """Add a view, but fail loudly and show the real cause."""
        try:
            v = admin_ui.add_view(view)
            if v is None:
                raise RuntimeError("admin_ui.add_view returned None")
            print(f"Registered view: {view.__class__.__name__} "
                  f"| endpoint={endpoint} | url={v.url}")
            return v
        except Exception as e:
            print(f"FAILED to register {view.__class__.__name__} (endpoint={endpoint})")
            traceback.print_exc()
            raise

    # Use explicit endpoints for each view to avoid Flask endpoint collisions
    add(UnitView(   BilSectionalUnit,     db.session, name="Units",          endpoint="data_units"),          endpoint="data_units")
    add(TenantView( BilTenant,            db.session, name="Tenants",        endpoint="data_tenants"),        endpoint="data_tenants")
    add(MeterView(  BilMeter,             db.session, name="Meters",         endpoint="data_meters"),         endpoint="data_meters")
    add(LeaseView(  BilLease,             db.session, name="Leases",         endpoint="data_leases"),         endpoint="data_leases")
    add(BaseView(   BilMeterReading,      db.session, name="Readings",       endpoint="data_readings"),       endpoint="data_readings")
    add(BaseView(   BilMeterFixedCharge,  db.session, name="Fixed Charges",  endpoint="data_fixed_charges"),  endpoint="data_fixed_charges")
