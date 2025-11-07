from flask_login import current_user
from app import db
from app.models.billing import BilProperty, BilSectionalUnit, BilTenant, BilMeter
from app.models.auth import User

def get_dashboard_data():
    """
    Fetches property/unit/meter data for the logged-in user
    based on their role (tenant or manager).
    """

    query = (
        db.session.query(
            BilProperty.id.label("property_id"),
            BilProperty.name.label("property_name"),
            BilProperty.address,
            BilSectionalUnit.id.label("unit_id"),
            BilSectionalUnit.unit_number,
            BilTenant.id.label("tenant_id"),
            BilTenant.name.label("tenant_name"),
            BilTenant.unit_label,
            BilMeter.id.label("meter_id"),
            BilMeter.meter_number,
            BilMeter.utility_type
        )
        .join(BilSectionalUnit, BilProperty.id == BilSectionalUnit.property_id)
        .join(BilTenant, BilSectionalUnit.id == BilTenant.sectional_unit_id)
        .join(User, BilTenant.user_id == User.id)
        .join(BilMeter, BilSectionalUnit.id == BilMeter.sectional_unit_id)
    )

    # Filter based on role
    if current_user.role == "tenant":
        query = query.filter(BilTenant.user_id == current_user.id)

    elif current_user.role == "manager":
        query = query.filter(BilProperty.manager_id == current_user.id)

    elif current_user.role == "admin":
        # Admin can see all billing data
        pass

    else:
        # Other roles: no billing data
        return []

    return query.all()
