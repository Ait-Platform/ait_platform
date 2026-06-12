import re

with open("D:/Users/yeshk/Documents/ait_platform/app/school_billing/routes.py", "r", encoding="utf-8") as f:
    content = f.read()

new_setup_submit = """@billing_bp.route("/billing/setup/submit", methods=["POST"])
@login_required
def setup_submit():
    payload_str = request.form.get("payload")
    if not payload_str:
        flash("Invalid payload", "danger")
        return redirect(url_for("billing_bp.setup_wizard"))
        
    try:
        import json
        data = json.loads(payload_str)
        
        # 1. Create Property
        prop_name = data.get("property_name", "").strip()
        if not prop_name:
            prop_name = "New Property"
            
        prop = BilProperty(
            name=prop_name,
            address=data.get("address"),
            manager_id=current_user.id,
            metro_arrangement_amount=float(data.get("metro_arrangement_amount") or 0.0),
            metro_arrangement_duration=int(data.get("metro_arrangement_duration") or 0),
            metro_rates_amount=float(data.get("metro_rates_amount") or 0.0)
        )
        db.session.add(prop)
        db.session.flush()
        
        # 2. Create Default Unit (Fix duplicate key crash)
        unit = BilSectionalUnit(
            property_id=prop.id,
            name=f"{prop_name} - Main Unit (Prop {prop.id})"
        )
        db.session.add(unit)
        db.session.flush()
        
        # 3. Conditionally Create Tenant & Lease
        tenant_name = data.get("tenant_name", "").strip()
        if tenant_name:
            tenant = BilTenant(
                name=tenant_name,
                email=data.get("tenant_email"),
                sectional_unit_id=unit.id
            )
            db.session.add(tenant)
            db.session.flush()
            
            rent_amount = data.get("rent_amount")
            lease = BilLease(
                tenant_id=tenant.id,
                sectional_unit_id=unit.id,
                rent_amount=float(rent_amount) if rent_amount else 0.0,
                tenant_arrangement_charge=float(data.get("tenant_arrangement_charge") or 0.0),
                tenant_rates_charge=float(data.get("tenant_rates_charge") or 0.0),
                tenant_arrears_total=float(data.get("tenant_arrears_total") or 0.0),
                tenant_arrears_installment=float(data.get("tenant_arrears_installment") or 0.0),
                agent_fee_amount=float(data.get("agent_fee_amount") or 0.0),
                agent_fee_target=str(data.get("agent_fee_target") or 'owner')
            )
            db.session.add(lease)
            
        # 5. Create Meters
        meter_objects = {}
        # First pass: Bulk Meters
        for m_data in data.get("meters", []):
            if m_data.get("hierarchy") == "bulk":
                meter = BilMeter(
                    meter_number=m_data.get("number"),
                    utility_type=m_data.get("type"),
                    sectional_unit_id=unit.id,
                    pointing_to=m_data.get("pointing_to"),
                    municipal_bill_number=m_data.get("municipal_bill_number")
                )
                db.session.add(meter)
                db.session.flush()
                meter_objects[m_data.get("temp_id")] = meter.id
                
        # Second pass: Sub-meters and Independent
        for m_data in data.get("meters", []):
            h = m_data.get("hierarchy")
            if h in ["independent", "sub"]:
                parent_id = None
                if h == "sub":
                    parent_temp_id = m_data.get("parent_id")
                    parent_id = meter_objects.get(parent_temp_id)
                            
                meter = BilMeter(
                    meter_number=m_data.get("number"),
                    utility_type=m_data.get("type"),
                    sectional_unit_id=unit.id,
                    parent_meter_id=parent_id,
                    pointing_to=m_data.get("pointing_to"),
                    municipal_bill_number=m_data.get("municipal_bill_number")
                )
                db.session.add(meter)
        
        db.session.commit()
        flash("Property successfully set up!", "success")
        return redirect(url_for("billing_bp.learner_dashboard"))
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Setup Wizard Error: {str(e)}")
        flash("An error occurred during setup.", "danger")
        return redirect(url_for("billing_bp.setup_wizard"))"""

# Use regex to replace the function
pattern = re.compile(r'@billing_bp\.route\("/billing/setup/submit", methods=\["POST"\]\)\s*@login_required\s*def setup_submit\(\):.*?(?=\Z)', re.DOTALL)
new_content = pattern.sub(new_setup_submit, content)

with open("D:/Users/yeshk/Documents/ait_platform/app/school_billing/routes.py", "w", encoding="utf-8") as f:
    f.write(new_content)
    
print("Updated setup_submit route successfully!")
