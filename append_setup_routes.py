import os

route_code = """
import json
from app.models.billing import BilLease

@billing_bp.route("/billing/setup", methods=["GET"])
@login_required
def setup_wizard():
    return render_template("school_billing/setup_wizard.html")

@billing_bp.route("/billing/setup/submit", methods=["POST"])
@login_required
def setup_submit():
    payload_str = request.form.get("payload")
    if not payload_str:
        flash("Invalid payload", "danger")
        return redirect(url_for("billing_bp.setup_wizard"))
        
    try:
        data = json.loads(payload_str)
        
        # 1. Create Property
        prop = BilProperty(
            name=data.get("property_name"),
            address=data.get("address"),
            municipal_bill_number=data.get("municipal_bill_number"),
            manager_id=current_user.id
        )
        db.session.add(prop)
        db.session.flush()
        
        # 2. Create Default Unit
        unit = BilSectionalUnit(
            property_id=prop.id,
            unit_number="Unit 1"
        )
        db.session.add(unit)
        db.session.flush()
        
        # 3. Create Tenant
        tenant = BilTenant(
            name=data.get("tenant_name"),
            email=data.get("tenant_email"),
            sectional_unit_id=unit.id
        )
        db.session.add(tenant)
        db.session.flush()
        
        # 4. Create Lease (for Rent Amount)
        rent_amount = data.get("rent_amount")
        if rent_amount:
            lease = BilLease(
                tenant_id=tenant.id,
                sectional_unit_id=unit.id,
                rent_amount=float(rent_amount)
            )
            db.session.add(lease)
            
        # 5. Create Meters
        meter_objects = []
        # First pass: Bulk Meters
        for i, m_data in enumerate(data.get("meters", [])):
            if m_data.get("hierarchy") == "bulk":
                meter = BilMeter(
                    meter_number=m_data.get("number"),
                    utility_type=m_data.get("type"),
                    sectional_unit_id=unit.id
                )
                db.session.add(meter)
                db.session.flush()
                meter_objects.append({'index': i, 'id': meter.id})
                
        # Second pass: Sub-meters and Independent
        for i, m_data in enumerate(data.get("meters", [])):
            h = m_data.get("hierarchy")
            if h in ["independent", "sub"]:
                parent_id = None
                if h == "sub":
                    # find the db id of the parent
                    parent_index = m_data.get("parent_id")
                    if parent_index is not None and parent_index != "":
                        parent_obj = next((m for m in meter_objects if str(m['index']) == str(parent_index)), None)
                        if parent_obj:
                            parent_id = parent_obj['id']
                            
                meter = BilMeter(
                    meter_number=m_data.get("number"),
                    utility_type=m_data.get("type"),
                    sectional_unit_id=unit.id,
                    parent_meter_id=parent_id
                )
                db.session.add(meter)
        
        db.session.commit()
        flash("Property successfully set up!", "success")
        return redirect(url_for("billing_bp.learner_dashboard"))
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Setup Wizard Error: {str(e)}")
        flash("An error occurred during setup.", "danger")
        return redirect(url_for("billing_bp.setup_wizard"))
"""

with open("D:/Users/yeshk/Documents/ait_platform/app/school_billing/routes.py", "a") as f:
    f.write("\n" + route_code + "\n")
print("Routes appended!")
