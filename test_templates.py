from app import create_app
from flask import render_template
from app.models.billing import BilProperty, BilSectionalUnit

app = create_app()

with app.test_request_context('/billing/property/1/view'):
    try:
        prop = BilProperty.query.first()
        units = BilSectionalUnit.query.filter_by(property_id=prop.id).all()
        render_template('school_billing/view_property.html', property=prop, units=units)
        print("View Template rendered successfully!")
    except Exception as e:
        import traceback
        traceback.print_exc()

with app.test_request_context('/billing/property/1/edit'):
    try:
        prop = BilProperty.query.first()
        render_template('school_billing/edit_property.html', property=prop, tenant=None, lease=None)
        print("Edit Template rendered successfully!")
    except Exception as e:
        import traceback
        traceback.print_exc()
