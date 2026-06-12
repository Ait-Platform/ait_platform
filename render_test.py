from app import create_app
from flask import render_template
from app.models.billing import BilProperty, BilTenant, BilConsumption, BilMeterReading, BilTariff, BilFixedItem
import traceback

app = create_app()

with app.app_context():
    from app.school_billing.routes import _generate_metsoa_data
    # Use property 8, tenant 8 (just need a valid tenant ID for property 8, let's find it)
    tenant = BilTenant.query.filter_by(property_id=8).first() if hasattr(BilTenant, 'property_id') else None
    if not tenant:
        from app.models.billing import BilSectionalUnit
        u = BilSectionalUnit.query.filter_by(property_id=8).first()
        if u and u.tenants:
            tenant = u.tenants[0]
            
    if tenant:
        try:
            prop = BilProperty.query.get(8)
            # Use '2026-05' to get the seeded data
            elec_data, water_data, grand_total = _generate_metsoa_data(tenant.id, '2026-05')
            
            with app.test_request_context():
                html = render_template(
                    "school_billing/metsoa.html",
                    tenant=tenant,
                    property=prop,
                    month='2026-05',
                    electricity=elec_data,
                    water=water_data,
                    grand_total=grand_total
                )
                with open('test_output.html', 'w', encoding='utf-8') as f:
                    f.write(html)
                print("Rendered to test_output.html")
        except Exception as e:
            traceback.print_exc()
    else:
        print("Tenant not found")
