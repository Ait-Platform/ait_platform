new_routes = '''
@billing_bp.route("/billing/soa", methods=["GET"])
@login_required
def soa_dashboard():
    from app.models.billing import BilProperty, BilSectionalUnit, BilTenant
    properties = BilProperty.query.filter_by(manager_id=current_user.id).all()
    
    data = []
    for prop in properties:
        units = BilSectionalUnit.query.filter_by(property_id=prop.id).all()
        prop_tenants = []
        for u in units:
            prop_tenants.extend(u.tenants)
        
        data.append({
            'property': prop,
            'tenants': prop_tenants
        })
        
    return render_template("program_billing/soa_dashboard.html", data=data)

@billing_bp.route("/billing/soa/tenant/<int:tenant_id>/edit", methods=["GET", "POST"])
@login_required
def edit_tenant_soa(tenant_id):
    from app.models.billing import BilTenant
    from app.extensions import db
    from datetime import datetime
    
    tenant = BilTenant.query.get_or_404(tenant_id)
    
    # Very basic validation that the user owns the property
    if tenant.sectional_unit and tenant.sectional_unit.property:
        if tenant.sectional_unit.property.manager_id != current_user.id and not current_user.has_role('admin'):
            abort(403)
            
    if request.method == "POST":
        tenant.address = request.form.get("address")
        
        ds = request.form.get("date_started")
        if ds:
            tenant.date_started = datetime.strptime(ds, "%Y-%m-%d").date()
            
        dt = request.form.get("date_terminated")
        if dt:
            tenant.date_terminated = datetime.strptime(dt, "%Y-%m-%d").date()
        else:
            tenant.date_terminated = None
            
        is_active = request.form.get("is_active") == "on"
        tenant.is_active = is_active
        
        db.session.commit()
        flash("Tenant SOA Configuration updated.", "success")
        return redirect(url_for("billing_bp.soa_dashboard"))
        
    return render_template("program_billing/edit_tenant_soa.html", tenant=tenant)

@billing_bp.route("/billing/soa/tenant/<int:tenant_id>/generate", methods=["GET"])
@login_required
def generate_soa(tenant_id):
    from app.models.billing import BilTenant
    tenant = BilTenant.query.get_or_404(tenant_id)
    # Placeholder for actual SOA generation logic which aggregates MetSoa + Arrears
    flash("SOA Generation logic to be implemented. This will pull METSOA charges and tenant arrears.", "info")
    return redirect(url_for("billing_bp.soa_dashboard"))
'''

with open('app/program_billing/routes.py', 'a', encoding='utf-8') as f:
    f.write('\n' + new_routes)
print('Done appending SOA routes')
