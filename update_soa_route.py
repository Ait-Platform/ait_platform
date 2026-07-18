import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_func = '''@billing_bp.route("/billing/soa", methods=["GET"])
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
        
    return render_template("program_billing/soa_dashboard.html", data=data)'''

new_func = '''@billing_bp.route("/billing/soa", methods=["GET", "POST"])
@login_required
def soa_dashboard():
    from app.models.billing import BilProperty, BilSectionalUnit, BilTenant, BilMuniAccount
    from datetime import datetime
    
    properties = BilProperty.query.filter_by(manager_id=current_user.id).all()
    current_month = datetime.now().strftime("%Y-%m")
    
    selected_prop_id = request.args.get('property_id') or request.form.get('property_id')
    selected_month = request.args.get('month') or request.form.get('month') or current_month
    
    selected_prop = None
    tenants = []
    muni_accounts = []
    
    if selected_prop_id:
        selected_prop = BilProperty.query.filter_by(id=selected_prop_id, manager_id=current_user.id).first()
        if selected_prop:
            units = BilSectionalUnit.query.filter_by(property_id=selected_prop.id).all()
            for u in units:
                tenants.extend(u.tenants)
            muni_accounts = BilMuniAccount.query.filter_by(property_id=selected_prop.id).all()
            
    return render_template(
        "program_billing/soa_dashboard.html", 
        properties=properties, 
        current_month=selected_month,
        selected_prop=selected_prop,
        tenants=tenants,
        muni_accounts=muni_accounts
    )

@billing_bp.route("/billing/soa/map/update", methods=["POST"])
@login_required
def update_soa_map():
    from app.models.billing import BilMuniAccount
    from app.extensions import db
    
    account_id = request.form.get("account_id")
    property_id = request.form.get("property_id")
    month = request.form.get("month")
    
    if not account_id:
        flash("Invalid account ID.", "danger")
        return redirect(url_for('billing_bp.soa_dashboard'))
        
    acc = BilMuniAccount.query.get_or_404(account_id)
    if acc.property and acc.property.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    acc.arrears_charge_to = request.form.get("arrears_charge_to", "owner")
    acc.rates_charge_to = request.form.get("rates_charge_to", "owner")
    acc.arrangement_charge_to = request.form.get("arrangement_charge_to", "owner")
    
    db.session.commit()
    flash("SOA Map updated successfully.", "success")
    return redirect(url_for("billing_bp.soa_dashboard", property_id=property_id, month=month))
'''

text = text.replace(old_func, new_func)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated routes.py')
