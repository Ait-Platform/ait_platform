import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# I will replace the existing ai_onboarding_process route
import re

# Find the start of the function
start_idx = content.find('def ai_onboarding_process():')

# Find the end of the function (the start of the next route)
end_idx = content.find('@billing_bp.route("/billing/onboarding/start_setup", methods=["POST"])', start_idx)

if start_idx != -1 and end_idx != -1:
    old_route = content[start_idx:end_idx]
    
    new_route = '''def ai_onboarding_process():
    data = request.json
    if not data:
        return jsonify({"error": "Invalid JSON payload"}), 400
        
    try:
        property_id = data.get("property_id")
        if not property_id:
            return jsonify({"error": "Missing property_id. You must start a setup from the dashboard first."}), 400
            
        prop = BilProperty.query.get(property_id)
        if not prop or prop.manager_id != current_user.id:
            return jsonify({"error": "Property not found or unauthorized"}), 403
            
        # 1. Update Property Details
        prop.address = data.get("address") or prop.address
        prop.metro_rates_amount = float(data.get("rates_amount") or 0.0)
        
        # 2. Setup Units & Tenants Map
        unit_map = {}
        tenant_map = {}
        
        # 2a. Owner Account (Default)
        owner_unit = BilSectionalUnit(property_id=prop.id, name=f"{prop.name} - Owner/Common")
        db.session.add(owner_unit)
        db.session.flush()
        unit_map["owner"] = owner_unit.id
        
        owner_tenant = BilTenant(name="Owner Account", sectional_unit_id=owner_unit.id)
        db.session.add(owner_tenant)
        db.session.flush()
        tenant_map["owner"] = owner_tenant.id
        db.session.add(BilLease(tenant_id=owner_tenant.id, sectional_unit_id=owner_unit.id, rent_amount=0))
        
        # 2b. Dynamic Tenants
        for t_data in data.get("tenants", []):
            tid = t_data.get("id")
            if not tid: continue
            tname = t_data.get("name", "").strip() or f"Statement {tid}"
            rent = float(t_data.get("rent") or 0.0)
            
            t_unit = BilSectionalUnit(property_id=prop.id, name=f"Unit {tid.upper()}")
            db.session.add(t_unit)
            db.session.flush()
            unit_map[tid] = t_unit.id
            
            t_tenant = BilTenant(name=tname, sectional_unit_id=t_unit.id)
            db.session.add(t_tenant)
            db.session.flush()
            tenant_map[tid] = t_tenant.id
            db.session.add(BilLease(tenant_id=t_tenant.id, sectional_unit_id=t_unit.id, rent_amount=rent))
            
        # 3. Create Meters and initial readings (baselines)
        month = data.get("month")
        from datetime import datetime
        if not month:
            month = datetime.now().strftime("%Y-%m")
            
        def _add_baseline(meter, m_data):
            last_date_str = m_data.get("previous_date")
            new_date_str = m_data.get("current_date")
            try:
                last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date() if last_date_str else datetime.utcnow().date()
            except:
                last_date = datetime.utcnow().date()
            try:
                new_date = datetime.strptime(new_date_str, "%Y-%m-%d").date() if new_date_str else datetime.utcnow().date()
            except:
                new_date = datetime.utcnow().date()
            
            last_read = float(m_data.get("previous_reading") or 0)
            new_read = float(m_data.get("current_reading") or 0)
            usage = float(m_data.get("usage") or 0)
            
            if usage == 0 and new_read > 0 and last_read > 0:
                usage = new_read - last_read
            
            days = (new_date - last_date).days
            if days <= 0: days = 30
            
            cons = BilConsumption(
                meter_id=meter.id,
                meter_number=meter.meter_number,
                month=month,
                last_date=last_date,
                new_date=new_date,
                last_read=last_read,
                new_read=new_read,
                days=days,
                consumption=usage
            )
            db.session.add(cons)

        bulk_meters = {} # utility_type -> meter.id
        
        # PASS 1: Bulk Meters
        for m_data in data.get("readings", []):
            if m_data.get("is_bulk"):
                assign_to = m_data.get("assign_to", "owner")
                u_id = unit_map.get(assign_to, owner_unit.id)
                u_type = (m_data.get("utility_type") or "water").lower()
                
                meter = BilMeter(
                    meter_number=m_data.get("meter_number"),
                    utility_type=u_type,
                    sectional_unit_id=u_id,
                    pointing_to="Entire Property",
                    municipal_bill_number=m_data.get("accountNo")
                )
                db.session.add(meter)
                db.session.flush()
                bulk_meters[u_type] = meter.id
                _add_baseline(meter, m_data)
                
        # PASS 2: Sub / Independent Meters
        for m_data in data.get("readings", []):
            if not m_data.get("is_bulk"):
                assign_to = m_data.get("assign_to", "owner")
                u_id = unit_map.get(assign_to, owner_unit.id)
                u_type = (m_data.get("utility_type") or "water").lower()
                
                parent_id = None
                if m_data.get("linked_to_bulk"):
                    parent_id = bulk_meters.get(u_type)
                
                meter = BilMeter(
                    meter_number=m_data.get("meter_number"),
                    utility_type=u_type,
                    sectional_unit_id=u_id,
                    parent_meter_id=parent_id,
                    pointing_to=None,
                    municipal_bill_number=m_data.get("accountNo")
                )
                db.session.add(meter)
                db.session.flush()
                _add_baseline(meter, m_data)
            
        # Advance the onboarding status to the next tile!
        prop.onboarding_status = 'draft_readings'
        
        db.session.commit()
        
        # Sync Municipality Accounts
        from app.program_billing.helpers import sync_muni_accounts
        sync_muni_accounts()
        
        # Return success as JSON since Alpine fetch is expecting it
        return jsonify({"success": True, "property_id": prop.id})
        
    except Exception as e:
        db.session.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"An error occurred during setup: {str(e)}"}), 500

'''
    content = content.replace(old_route, new_route)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Successfully replaced ai_onboarding_process")
else:
    print("Could not find boundaries")
