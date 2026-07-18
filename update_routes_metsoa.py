import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix utilities_hub action "metsoa"
utilities_action_old = '''        elif action == "metsoa":
            from app.models.billing import BilSectionalUnit
            units = BilSectionalUnit.query.filter_by(property_id=property_id).all()
            tenant_id = None
            if units and units[0].tenants:
                tenant_id = units[0].tenants[0].id
                
            if tenant_id:
                return redirect(url_for("billing_bp.metsoa", tenant_id=tenant_id, month=month))
            else:
                flash("No tenants found for this property to generate METSOA.", "danger")
                return redirect(url_for("billing_bp.utilities_hub"))'''

utilities_action_new = '''        elif action == "metsoa":
            return redirect(url_for("billing_bp.metsoa", property_id=property_id, month=month))'''

text = text.replace(utilities_action_old, utilities_action_new)

# Fix metsoa route to accept property_id and remove ledger auto-post
metsoa_route_old = '''@billing_bp.route("/billing/metsoa/<int:tenant_id>/<month>")
@login_required
def metsoa(tenant_id, month):
    tenant = BilTenant.query.get(tenant_id)
    if not tenant:
        flash("Tenant not found", "danger")
        return redirect(url_for("billing_bp.learner_dashboard"))

    prop = BilProperty.query.get(tenant.sectional_unit.property_id) if tenant.sectional_unit and tenant.sectional_unit.property_id else None'''

metsoa_route_new = '''@billing_bp.route("/billing/metsoa/<int:property_id>/<month>")
@login_required
def metsoa(property_id, month):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)'''

text = text.replace(metsoa_route_old, metsoa_route_new)

# Remove the tenant requirement from the ledger auto-post logic and build_electrical_rows
metsoa_body_old = '''    elec_rows, elec_total = build_electrical_rows(tenant.sectional_unit_id, month)
    water_meters, water_total = build_water_rows(tenant.sectional_unit_id, month)
    
    grand_total = round(elec_total + water_total, 2)
    
    # Auto-post to ledger
    _auto_post_to_ledger(tenant_id, month, grand_total, tenant, elec_rows)
    
    return render_template("program_billing/metsoa.html", 
                           tenant=tenant, 
                           property=prop,
                           month=month,
                           elec_rows=elec_rows,
                           elec_total=elec_total,
                           water_meters=water_meters,
                           water_total=water_total,
                           grand_total=grand_total)'''

metsoa_body_new = '''    elec_rows, elec_total = build_electrical_rows(prop.id, month)
    water_meters, water_total = build_water_rows(prop.id, month)
    
    grand_total = round(elec_total + water_total, 2)
    
    # Auto-post to ledger removed (Option A: decoupled METSOA)
    
    return render_template("program_billing/metsoa.html", 
                           tenant=None, 
                           property=prop,
                           month=month,
                           elec_rows=elec_rows,
                           elec_total=elec_total,
                           water_meters=water_meters,
                           water_total=water_total,
                           grand_total=grand_total)'''

text = text.replace(metsoa_body_old, metsoa_body_new)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated routes.py')
