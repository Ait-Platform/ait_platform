import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update soa_dashboard to handle POST and redirect
old_soa_dash = '''@billing_bp.route("/billing/soa", methods=["GET", "POST"])
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
    )'''

new_soa_dash = '''@billing_bp.route("/billing/soa", methods=["GET", "POST"])
@login_required
def soa_dashboard():
    from app.models.billing import BilProperty
    from datetime import datetime
    
    properties = BilProperty.query.filter_by(manager_id=current_user.id).all()
    current_month = datetime.now().strftime("%Y-%m")
    
    if request.method == "POST":
        property_id = request.form.get("property_id")
        month = request.form.get("month")
        action = request.form.get("action")
        
        if not property_id or not month:
            flash("Please select both a property and a month.", "warning")
            return redirect(url_for("billing_bp.soa_dashboard"))
            
        if action == "charge_map":
            return redirect(url_for("billing_bp.soa_map_view", property_id=property_id, month=month))
        elif action == "tenants":
            return redirect(url_for("billing_bp.soa_tenants_view", property_id=property_id, month=month))
        elif action == "generate_soa":
            return redirect(url_for("billing_bp.soa_generate_view", property_id=property_id, month=month))
            
    return render_template("program_billing/soa_dashboard.html", properties=properties, current_month=current_month)

@billing_bp.route("/billing/soa/map/<int:property_id>/<month>", methods=["GET"])
@login_required
def soa_map_view(property_id, month):
    from app.models.billing import BilProperty, BilMuniAccount
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
    muni_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    return render_template("program_billing/soa_map.html", property=prop, month=month, muni_accounts=muni_accounts)

@billing_bp.route("/billing/soa/tenants/<int:property_id>/<month>", methods=["GET"])
@login_required
def soa_tenants_view(property_id, month):
    from app.models.billing import BilProperty, BilSectionalUnit
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
    units = BilSectionalUnit.query.filter_by(property_id=prop.id).all()
    tenants = []
    for u in units:
        tenants.extend(u.tenants)
    return render_template("program_billing/soa_tenants.html", property=prop, month=month, tenants=tenants)

@billing_bp.route("/billing/soa/generate/<int:property_id>/<month>", methods=["GET"])
@login_required
def soa_generate_view(property_id, month):
    from app.models.billing import BilProperty, BilSectionalUnit
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
    units = BilSectionalUnit.query.filter_by(property_id=prop.id).all()
    tenants = []
    for u in units:
        tenants.extend(u.tenants)
    return render_template("program_billing/soa_generate.html", property=prop, month=month, tenants=tenants)
'''

text = text.replace(old_soa_dash, new_soa_dash)

# 2. Update update_soa_map route redirect
old_map_update = '''flash("SOA Map updated successfully.", "success")
    return redirect(url_for("billing_bp.soa_dashboard", property_id=property_id, month=month))'''
new_map_update = '''flash("SOA Map updated successfully.", "success")
    return redirect(url_for("billing_bp.soa_map_view", property_id=property_id, month=month))'''
text = text.replace(old_map_update, new_map_update)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("Updated routes.py")
