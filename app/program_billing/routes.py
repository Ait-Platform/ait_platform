
import tempfile
import os
from flask import send_file, request

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, abort
from app.extensions import db
from flask import current_app
from datetime import datetime, timedelta
# Models (clean and complete)
from app.models.billing import (
    BilProperty, BilTenant, BilMeter, BilMeterReading,
    BilConsumption, BilTariff, BilProperty, 
    BilMeterFixedCharge, PropertyForm
    )
from flask_login import login_user, logout_user, login_required, current_user
from app.models.auth import User
from werkzeug.security import check_password_hash, generate_password_hash
import hashlib
from sqlalchemy import text
from flask import jsonify
import time
from app.auth.forms import RegisterForm, ManagerPropertyForm, TenancyForm
from app.program_billing.helpers import get_dashboard_data

def _get_billing_enrollment_id(user_id):
    from app.models.auth import UserEnrollment, AuthSubject
    subj = AuthSubject.query.filter_by(slug='billing').first()
    if not subj: return None
    enr = UserEnrollment.query.filter_by(user_id=user_id, subject_id=subj.id).first()
    return enr.id if enr else None

billing_bp = Blueprint('billing_bp', __name__)

@billing_bp.route('/billing/checkout/<month>', methods=['GET'])
@login_required
def billing_checkout(month):
    from app.models.billing import BilStatementPayment, BilProperty, BilProperty, BilMeter, BilPlatformSettings
    # Check if already paid
    payment = BilStatementPayment.query.filter_by(manager_id=current_user.id, month=month).first()
    if payment and payment.amount_paid_cents > 0:
        flash("You have already unlocked statements for this month.", "info")
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    # Calculate meters
    props = BilProperty.query.filter_by(manager_id=current_user.id).all()
    prop_ids = [p.id for p in props]
    
    if not prop_ids:
        flash("You have no properties to bill.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    units = BilProperty.query.filter(BilProperty.property_id.in_(prop_ids)).all()
    unit_ids = [property.id for u in units]
    
    meters = BilMeter.query.filter(BilMeter.property_id.in_(unit_ids)).all()
    meter_count = len(meters)
    
    settings = BilPlatformSettings.query.first()
    if not settings:
        settings = BilPlatformSettings(base_price_cents=10000, included_meters=2, extra_meter_price_cents=1500)
        db.session.add(settings)
        db.session.commit()
        
    cost_cents = settings.base_price_cents
    if meter_count > settings.included_meters:
        extra_meters = meter_count - settings.included_meters
        cost_cents += extra_meters * settings.extra_meter_price_cents
        
    session["metro_billing_month"] = month
    session["metro_billing_meters"] = meter_count
    session["metro_billing_amount_cents"] = cost_cents
    
    main_prop = props[0] if props else None
    from datetime import datetime
    # Hardcode trial to True for testing
    is_trial = True
    
    return render_template("program_billing/checkout_summary.html", 
                           month=month, 
                           meter_count=meter_count, 
                           cost_cents=cost_cents, 
                           settings=settings,
                           main_prop=main_prop,
                           is_trial=is_trial)

@billing_bp.route('/billing/about')
def billing_about():
    from app.models.billing import BilPlatformSettings
    settings = BilPlatformSettings.query.first()
    if not settings:
        settings = BilPlatformSettings(base_price_cents=10000, included_meters=2, extra_meter_price_cents=1500)
        from app.extensions import db
        db.session.add(settings)
        db.session.commit()
    return render_template("program_billing/about.html", settings=settings)

@billing_bp.route('/billing/price')
def billing_price():
    from app.models.billing import BilPlatformSettings
    settings = BilPlatformSettings.query.first()
    if not settings:
        settings = BilPlatformSettings(
            base_price_cents=10000,
            included_meters=2,
            extra_meter_price_cents=2000
        )
        db.session.add(settings)
        db.session.commit()
    return render_template("program_billing/price.html", settings=settings)

@billing_bp.route("/billing/portfolio", methods=["GET", "POST"])
@login_required
def property_portfolio():

    tenant_id = None
    if units and units[0].tenants:
        tenant_id = units[0].tenants[0].id

    if request.method == "POST":
        from app.models.billing import BilProperty
        # Create property + unit in one go
        prop = BilProperty(
            name=request.form["property_name"],
            address=request.form.get("address", ""),
            description=request.form.get("description"),
            manager_id=current_user.id,
            enrollment_id=_get_billing_enrollment_id(current_user.id)
        )
        db.session.add(prop)
        db.session.flush()

        unit = BilProperty(
            property_id=prop.id,
            unit_number=request.form.get("unit_number", "1")
        )
        db.session.add(unit)
        db.session.commit()

        flash("Property and unit added successfully!", "success")
        return redirect(url_for("billing_bp.property_portfolio"))

    # Normal GET show dashboard data
    data = get_dashboard_data()

    return render_template("program_billing/property_portfolio.html", data=data)

@billing_bp.route("/billing/dashboard", methods=["GET"])
@billing_bp.route("/billing/home", endpoint="subject_home", methods=["GET"])
@login_required
def learner_dashboard():
    # Normal GET show dashboard data
    data = get_dashboard_data()

    return render_template("program_billing/manager_dashboard.html", data=data)

from app.models.billing import BilLease, BilPayment

@billing_bp.route("/billing/property/<int:property_id>/delete", methods=["POST"])
@login_required
def delete_property(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    try:
        # Manually cascade delete to be safe
        meters = BilMeter.query.filter_by(property_id=prop.id).all()
        for m in meters:
            BilMeterReading.query.filter_by(meter_id=m.id).delete()
            BilConsumption.query.filter_by(meter_id=m.id).delete()
            BilMeterFixedCharge.query.filter_by(meter_id=m.id).delete()
            db.session.delete(m)
                
        tenants = BilTenant.query.filter_by(property_id=prop.id).all()
        for t in tenants:
            BilLease.query.filter_by(tenant_id=t.id).delete()
                # Assuming statements are cascading or don't exist yet
            db.session.delete(t)
                

            
        BilPayment.query.filter_by(property_id=prop.id).delete()
        db.session.delete(prop)
        db.session.commit()
        flash("Property and all its records deleted securely.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error deleting property: {e}", "danger")
        
    return redirect(url_for("billing_bp.property_portfolio"))

@billing_bp.route("/billing/property/<int:property_id>/view", methods=["GET"])
@login_required
def view_property(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    return render_template("program_billing/view_property.html", property=prop, account_meters=account_meters)

@billing_bp.route("/billing/property/<int:property_id>/edit", methods=["GET", "POST"])
@login_required
def edit_property(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    # Get primary unit and tenant if exists
    tenant = BilTenant.query.filter_by(property_id=prop.id).first()
    lease = None
    if tenant:
        if tenant:
            lease = BilLease.query.filter_by(tenant_id=tenant.id).first()
            

    tenant_id = None
    if units and units[0].tenants:
        tenant_id = units[0].tenants[0].id

    if request.method == "POST":
        prop.name = request.form.get("property_name")
        prop.address = request.form.get("address")
        prop.metro_arrangement_amount = float(request.form.get("metro_arrangement_amount") or 0)
        prop.metro_arrangement_duration = int(request.form.get("metro_arrangement_duration") or 0)
        prop.metro_rates_amount = float(request.form.get("metro_rates_amount") or 0)
        
        if tenant:
            tenant.name = request.form.get("tenant_name")
            tenant.email = request.form.get("tenant_email")
        
        if lease:
            lease.rent_amount = float(request.form.get("rent_amount") or 0)
            lease.tenant_arrangement_charge = float(request.form.get("tenant_arrangement_charge") or 0)
            lease.tenant_rates_charge = float(request.form.get("tenant_rates_charge") or 0)
            lease.tenant_arrears_total = float(request.form.get("tenant_arrears_total") or 0)
            lease.tenant_arrears_installment = float(request.form.get("tenant_arrears_installment") or 0)
            lease.agent_fee_target = request.form.get("agent_fee_target") or "owner"
            
        import json
        
        # 1. Handle Existing Meters updates
        existing_meters_json = request.form.get("existing_meters_json")
        if existing_meters_json:
            try:
                existing_data = json.loads(existing_meters_json)
                for m_data in existing_data:
                    m_id = m_data.get("id")
                    if m_id:
                        meter = BilMeter.query.get(m_id)
                        if meter and meter.property_id == property.id:
                            meter.meter_number = m_data.get("number", meter.meter_number)
                            meter.pointing_to = m_data.get("pointing_to", meter.pointing_to)
                            meter.utility_type = m_data.get("type", meter.utility_type)
            except Exception as e:
                print(f"Error updating existing meters: {e}")
                flash("Error updating existing meters", "warning")
                
        
        # Handle Deleted Meters
        deleted_meters_json = request.form.get("deleted_meters_json")
        if deleted_meters_json:
            try:
                deleted_ids = json.loads(deleted_meters_json)
                for m_id in deleted_ids:
                    meter = BilMeter.query.get(m_id)
                    if meter and meter.property_id == property.id:
                        # Optional: also delete linked consumptions if cascade is not set
                        from app.models.billing import BilConsumption
                        
                        # Unlink sub-meters to prevent ForeignKeyViolation
                        sub_meters = BilMeter.query.filter_by(parent_meter_id=meter.id).all()
                        for sm in sub_meters:
                            sm.parent_meter_id = None
                            
                        BilConsumption.query.filter_by(meter_id=meter.id).delete()
                        db.session.delete(meter)
            except Exception as e:
                print(f"Error deleting meters: {e}")
                
        # 2. Handle New Meters

        new_meters_json = request.form.get("new_meters_json")
        print(f"DEBUG new_meters_json: {new_meters_json}")
        if new_meters_json and unit:
            try:
                meters_data = json.loads(new_meters_json)
                for m_data in meters_data:
                    number = m_data.get("number")
                    if number:
                        new_meter = BilMeter(
                            property_id=prop.id,
                            meter_number=number,
                            utility_type=m_data.get("type", "water"),
                            pointing_to=m_data.get("pointing_to", "")
                        )
                        db.session.add(new_meter)
            except Exception as e:
                print(f"Error adding new meters: {e}")
                flash(f"Failed to add new meters: {e}", "warning")
            
        db.session.commit()
        flash("Property settings updated!", "success")
        return redirect(url_for("billing_bp.learner_dashboard"))
        
    return render_template("program_billing/edit_property.html", property=prop, account_meters=account_meters, tenant=tenant, lease=lease, units=units)

@billing_bp.route("/billing/property/<int:property_id>/input_readings", methods=["GET", "POST"])
@login_required
def property_hub(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    all_meters = BilMeter.query.filter_by(property_id=property_id).all()
        
    # Also collect master meters by municipal account numbers
    from app.models.billing import BilMuniAccount, BilMeter
    muni_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    muni_acc_numbers = [acc.account_number for acc in muni_accounts if acc.account_number]
    
    if muni_acc_numbers:
        muni_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(muni_acc_numbers)).all()
        for m in muni_meters:
            if m not in all_meters:
                all_meters.append(m)
                
    # Also grab meters directly mapped by water_meter_id / elec_meter_id if any exist
    for acc in muni_accounts:
        if acc.water_meter and acc.water_meter not in all_meters:
            all_meters.append(acc.water_meter)
        if acc.elec_meter and acc.elec_meter not in all_meters:
            all_meters.append(acc.elec_meter)
        
    from datetime import datetime
    import calendar
    

    tenant_id = None
    if units and units[0].tenants:
        tenant_id = units[0].tenants[0].id

    if request.method == "POST":
        reading_month = request.form.get("reading_month")
        if not reading_month:
            flash("Reading month is required.", "danger")
            return redirect(request.url)
            
        added_count = 0
        for m in all_meters:
            val_str = request.form.get(f"reading_{m.id}")
            date_str = request.form.get(f"date_{m.id}")
            
            # Handle Baseline (Previous) Reading if submitted
            prev_val_str = request.form.get(f"prev_reading_{m.id}")
            prev_date_str = request.form.get(f"prev_date_{m.id}")
            
            # If the user provided a baseline, save it first
            if prev_val_str and prev_val_str.strip() and prev_date_str and prev_date_str.strip():
                prev_val = float(prev_val_str)
                prev_date = datetime.strptime(prev_date_str, "%Y-%m-%d").date()
                
                # Create the baseline reading
                baseline_read_obj = BilMeterReading(
                    meter_id=m.id,
                    reading_date=prev_date,
                    reading_value=prev_val
                )
                db.session.add(baseline_read_obj)
                db.session.flush() # Ensure it's available for the new reading logic below
            
            if val_str and val_str.strip() and date_str and date_str.strip():
                new_val = float(val_str)
                new_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                
                # Fetch the latest reading before or equal to this new_date
                last_reading = BilMeterReading.query.filter(
                    BilMeterReading.meter_id == m.id,
                    BilMeterReading.reading_date <= new_date # Allow same day if baseline was just added
                ).order_by(BilMeterReading.reading_date.desc()).first()
                
                # Prevent duplicate entries on exact same day if it's the exact same value
                if last_reading and last_reading.reading_date == new_date and last_reading.reading_value == new_val:
                    continue
                
                # Create the new reading
                new_read_obj = BilMeterReading(
                    meter_id=m.id,
                    reading_date=new_date,
                    reading_value=new_val
                )
                db.session.add(new_read_obj)
                
                # If there is a previous reading, calculate consumption
                if last_reading and last_reading.reading_date < new_date:
                    days = (new_date - last_reading.reading_date).days
                    if days > 0:
                        consumption_val = new_val - last_reading.reading_value
                        
                        # Only save positive consumption (if meter rolled over or replaced, needs manual adjustment)
                        if consumption_val >= 0:
                            cons_obj = BilConsumption(
                                meter_id=m.id,
                                meter_number=m.meter_number,
                                last_date=last_reading.reading_date,
                                new_date=new_date,
                                last_read=last_reading.reading_value,
                                new_read=new_val,
                                days=days,
                                consumption=consumption_val,
                                month=reading_month
                            )
                            db.session.add(cons_obj)
                added_count += 1
                
        if added_count > 0:
            db.session.commit()
            flash(f"Successfully saved {added_count} meter reading(s) for {reading_month}!", "success")
            
            # Redirect to the Property Hub
            return redirect(url_for('billing_bp.property_hub', property_id=property_id))
        else:
            flash("No readings were entered.", "warning")
            
        return redirect(url_for('billing_bp.property_hub', property_id=property_id))
        
    # GET: Prepare data for template
    passed_date = request.args.get('date')
    if passed_date:
        try:
            # Parse just to validate format, then use it
            dt = datetime.strptime(passed_date, "%Y-%m-%d")
            current_date = passed_date
            current_month = dt.strftime("%Y-%m")
        except ValueError:
            current_date = datetime.now().strftime("%Y-%m-%d")
            current_month = datetime.now().strftime("%Y-%m")
    else:
        current_date = datetime.now().strftime("%Y-%m-%d")
        current_month = datetime.now().strftime("%Y-%m")
        
    meters_data = []
    for m in all_meters:
        # Determine hierarchy
        is_bulk = False
        for om in all_meters:
            if om.parent_meter_id == m.id:
                is_bulk = True
                break
        hierarchy = 'Bulk' if is_bulk else ('Sub-Meter' if m.parent_meter_id else 'Independent')
        
        # Get latest reading
        last_reading = BilMeterReading.query.filter_by(meter_id=m.id).order_by(BilMeterReading.reading_date.desc()).first()
        
        # Calculate Average Consumption for validation
        consumptions = BilConsumption.query.filter_by(meter_id=m.id).all()
        avg_cons = 0
        if consumptions:
            total_cons = sum(c.consumption for c in consumptions)
            avg_cons = total_cons / len(consumptions)
        
        # Check if there is already a consumption record for the current month
        c_this_month = BilConsumption.query.filter_by(meter_id=m.id, month=current_month).first()
        new_read = c_this_month.new_read if c_this_month else ''
        new_date = c_this_month.new_date.strftime("%Y-%m-%d") if c_this_month and c_this_month.new_date else current_date
        last_read = c_this_month.last_read if c_this_month else ''
        last_date = c_this_month.last_date.strftime("%Y-%m-%d") if c_this_month and c_this_month.last_date else ''

        meters_data.append({
            'meter': m,
            'meter_number': m.meter_number,
            'utility_type': m.utility_type,
            'pointing_to': m.pointing_to,
            'hierarchy': hierarchy,
            'last_reading': last_reading,
            'avg_cons': round(avg_cons, 2),
            'new_read': new_read,
            'new_date': new_date,
            'last_read': last_read,
            'last_date': last_date
        })
        
    return render_template("program_billing/property_hub.html",
                           tenant_id=tenant_id, 
                           property=prop, 
                           meters_data=meters_data,
                           current_month=current_month,
                           current_date=current_date)

@billing_bp.route("/billing/consumption/<int:consumption_id>/delete", methods=["POST"])
@login_required
def delete_consumption(consumption_id):
    cons = BilConsumption.query.get_or_404(consumption_id)
    
    # Verify ownership
    meter = BilMeter.query.get(cons.meter_id)
    if meter and meter.property and meter.property_id:
        prop = BilProperty.query.get(meter.property_id)
        if prop.manager_id != current_user.id and not current_user.has_role('admin'):
            abort(403)
            
        # Delete the associated new reading as well to completely revert
        reading = BilMeterReading.query.filter_by(meter_id=cons.meter_id, reading_date=cons.new_date).first()
        if reading:
            db.session.delete(reading)
            
        db.session.delete(cons)
        db.session.commit()
        flash("Reading and consumption deleted. You can now re-enter it.", "success")
        return redirect(url_for('billing_bp.property_hub', property_id=prop.id))
        
    abort(404)




@billing_bp.route("/dashboard/admin", methods=["GET", "POST"])
@login_required
def admin_dashboard():
    return render_template("program_billing/admin_dashboard.html")

# --- Frontend Recon Statement ---
@billing_bp.route("/billing/muni/recon_statement", methods=["GET"])
@login_required
def muni_recon_statement():
    from datetime import datetime
    from app.program_billing.helpers import sync_muni_accounts
    
    # Auto-sync accounts before querying
    sync_muni_accounts()
    
    # 1. Get user's accounts
    acc_sql = """
        SELECT DISTINCT a.id, a.account_number, 
               wm.meter_number as muni_water_meter_no, 
               em.meter_number as muni_elec_meter_no, 
               o.name as owner_name, p.name as property_name
        FROM bil_muni_account a
        LEFT JOIN bil_meter wm ON wm.id = a.water_meter_id
        LEFT JOIN bil_meter em ON em.id = a.elec_meter_id
        JOIN bil_meter m ON (m.id = a.water_meter_id OR m.id = a.elec_meter_id)
        JOIN bil_property u ON property.id = m.property_id
        JOIN bil_property p ON p.id = u.property_id
        LEFT JOIN ref_muni_owner o ON o.id = a.owner_id
        WHERE p.manager_id = :manager_id
        ORDER BY a.account_number
    """
    user_accounts = db.session.execute(text(acc_sql), {"manager_id": current_user.id}).mappings().all()
    
    selected_account_num = request.args.get("account_number", "").strip()
    
    # Auto-select if there's only one account
    if not selected_account_num and len(user_accounts) == 1:
        selected_account_num = user_accounts[0]["account_number"]
        
    selected_account = next((acc for acc in user_accounts if acc["account_number"] == selected_account_num), None)
    
    rows = []
    default_metsoa = 0.0
    
    if selected_account:
        acc_id = selected_account["id"]
        period_sql = """
            SELECT p.period AS month,
                   COALESCE(mc.metsoa_due, ai.total_due, 0) AS metsoa_charges,
                   COALESCE(mct.due, 0) AS metro_due,
                   COALESCE(pay.total_paid, 0) AS owner_payment,
                   (COALESCE(mct.due, 0) - COALESCE(mc.metsoa_due, ai.total_due, 0)) AS difference,
                   pay.last_payment_date
            FROM (
                SELECT DISTINCT period FROM bil_muni_cycle_totals WHERE account_id = :acc_id
                UNION
                SELECT DISTINCT month as period FROM bil_muni_payment WHERE account_id = :acc_id
                UNION
                SELECT DISTINCT period FROM bil_metsoa_cycle WHERE account_id = :acc_id
                UNION
                SELECT DISTINCT l.month as period 
                FROM bil_tenant_ledger l
                JOIN bil_tenant t ON t.id = l.tenant_id
                WHERE l.ref = 'METSOA-AUTO' AND EXISTS (
                    SELECT 1 FROM bil_meter m WHERE m.property_id = t.property_id AND m.municipal_bill_number = :acc_num
                )
            ) p
            LEFT JOIN bil_metsoa_cycle mc ON mc.account_id = :acc_id AND mc.period = p.period
            LEFT JOIN bil_muni_cycle_totals mct ON mct.account_id = :acc_id AND mct.period = p.period
            LEFT JOIN (
                SELECT l.month, SUM(l.amount) as total_due
                FROM bil_tenant_ledger l
                JOIN bil_tenant t ON t.id = l.tenant_id
                WHERE l.ref = 'METSOA-AUTO' AND EXISTS (
                    SELECT 1 FROM bil_meter m WHERE m.property_id = t.property_id AND m.municipal_bill_number = :acc_num
                )
                GROUP BY l.month
            ) ai ON ai.month = p.period
            LEFT JOIN (
                SELECT month, SUM(amount) as total_paid, MAX(payment_date) as last_payment_date
                FROM bil_muni_payment 
                WHERE account_id = :acc_id 
                GROUP BY month
            ) pay ON pay.month = p.period
            ORDER BY p.period DESC
        """
        rows = db.session.execute(db.text(period_sql), {
            "acc_id": acc_id,
            "acc_num": selected_account.get("account_number")
        }).mappings().all()
        
        # Calculate default metsoa from property
        prop_sql = """
            SELECT p.metro_rates_amount, p.metro_arrangement_amount
            FROM bil_meter m
            JOIN bil_property u ON property.id = m.property_id
            JOIN bil_property p ON p.id = u.property_id
            WHERE m.id = :water_id OR m.id = :elec_id
            LIMIT 1
        """
        prop_data = db.session.execute(text(prop_sql), {
            "water_id": selected_account.get("water_meter_id"), 
            "elec_id": selected_account.get("elec_meter_id")
        }).mappings().first()
        
        if prop_data:
            default_metsoa = (prop_data.get("metro_rates_amount") or 0.0) + (prop_data.get("metro_arrangement_amount") or 0.0)

    current_month = datetime.now().strftime("%Y-%m")
        
    return render_template("program_billing/recon_statement.html", 
                           user_accounts=user_accounts, 
                           selected_account=selected_account,
                           rows=rows,
                           current_month=current_month,
                           default_metsoa=default_metsoa)

@billing_bp.route("/billing/muni/payment", methods=["POST"])
@login_required
def muni_payment_submit():
    from datetime import datetime
    from app.models.billing import BilMuniAccount, BilMuniPayment, BilMuniCycleTotals, BilMetsoaCycle
    
    account_number = request.form.get("account_number")
    date_str = request.form.get("date") # Full date e.g. YYYY-MM-DD
    amount = request.form.get("amount", type=float)
    metsoa_val = request.form.get("metsoa", type=float)
    metro_due = request.form.get("metro_due", type=float)
    
    if not account_number or not date_str:
        flash("Missing required fields", "danger")
        return redirect(url_for("billing_bp.muni_recon_statement", account_number=account_number))
        
    month = date_str[:7] # Extract YYYY-MM
    try:
        payment_date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        payment_date_obj = datetime.now().date()
        
    acc = BilMuniAccount.query.filter_by(account_number=account_number).first()
    if not acc:
        flash("Account not found", "danger")
        return redirect(url_for("billing_bp.muni_recon_statement", account_number=account_number))
        
    if amount is not None and amount > 0:
        # Check if payment exists for this month, update it if so
        payment = BilMuniPayment.query.filter_by(account_id=acc.id, month=month).first()
        if payment:
            payment.amount = amount
            payment.payment_date = payment_date_obj
        else:
            payment = BilMuniPayment(
                account_id=acc.id,
                month=month,
                payment_date=payment_date_obj,
                amount=amount,
                reference="Frontend user payment"
            )
            db.session.add(payment)
            
    # Always update or create the metsoa cycle if a value was provided
    if metsoa_val is not None:
        metsoa = BilMetsoaCycle.query.filter_by(account_id=acc.id, period=month).first()
        if metsoa:
            metsoa.metsoa_due = metsoa_val
        else:
            metsoa = BilMetsoaCycle(
                account_id=acc.id,
                period=month,
                metsoa_due=metsoa_val
            )
            db.session.add(metsoa)
    
    # Also update cycle totals for recon
    if (amount is not None and amount > 0) or metro_due is not None:
        cycle = BilMuniCycleTotals.query.filter_by(account_id=acc.id, period=month).first()
        if cycle:
            if amount is not None and amount > 0:
                cycle.paid = amount
            if metro_due is not None:
                cycle.due = metro_due
        else:
            cycle = BilMuniCycleTotals(
                account_id=acc.id,
                period=month,
                paid=amount if amount is not None else 0,
                due=metro_due if metro_due is not None else 0
            )
            db.session.add(cycle)
        
    db.session.commit()
    flash("Record saved successfully", "success")
    return redirect(url_for("billing_bp.muni_recon_statement", account_number=account_number))

@billing_bp.route("/billing/muni/payment/delete", methods=["POST"])
@login_required
def muni_payment_delete():
    from app.models.billing import BilMuniAccount, BilMuniPayment
    account_number = request.form.get("account_number")
    month = request.form.get("month")
    
    acc = BilMuniAccount.query.filter_by(account_number=account_number).first()
    if acc:
        payments = BilMuniPayment.query.filter_by(account_id=acc.id, month=month).all()
        for p in payments:
            db.session.delete(p)
        db.session.commit()
        flash(f"Payment record for {month} removed.", "success")
        
    return redirect(url_for("billing_bp.muni_recon_statement", account_number=account_number))

@billing_bp.route("/billing/muni/recon/edit", methods=["GET"])
@login_required
def muni_recon_edit():
    from app.models.billing import BilMuniAccount, BilMuniPayment, BilMuniCycleTotals, BilMetsoaCycle
    from sqlalchemy import text
    from datetime import datetime
    
    account_number = request.args.get("account_number")
    month = request.args.get("month")
    
    is_new = False
    if not month:
        month = datetime.now().strftime("%Y-%m")
        is_new = True
        
    if not account_number:
        flash("Account required", "danger")
        return redirect(url_for("billing_bp.muni_recon_statement"))
        
    acc = BilMuniAccount.query.filter_by(account_number=account_number).first()
    if not acc:
        flash("Account not found", "danger")
        return redirect(url_for("billing_bp.muni_recon_statement"))
        
    metsoa_charges = 0.0
    metro_due = 0.0
    owner_payment = 0.0
    
    mc = BilMetsoaCycle.query.filter_by(account_id=acc.id, period=month).first()
    if mc and mc.metsoa_due:
        metsoa_charges = float(mc.metsoa_due)
    else:
        ledger_sql = """
            SELECT SUM(l.amount) 
            FROM bil_tenant_ledger l
            JOIN bil_tenant t ON t.id = l.tenant_id
            WHERE l.ref = 'METSOA-AUTO' AND l.month = :month AND EXISTS (
                SELECT 1 FROM bil_meter m WHERE m.property_id = t.property_id AND m.municipal_bill_number = :acc_num
            )
        """
        val = db.session.execute(text(ledger_sql), {"month": month, "acc_num": account_number}).scalar()
        if val:
            metsoa_charges = float(val)
            
    mct = BilMuniCycleTotals.query.filter_by(account_id=acc.id, period=month).first()
    if mct and mct.due:
        metro_due = float(mct.due)
        
    pay = db.session.execute(text("SELECT SUM(amount) FROM bil_muni_payment WHERE account_id = :acc_id AND month = :month"), {"acc_id": acc.id, "month": month}).scalar()
    if pay:
        owner_payment = float(pay)
        
    return render_template("program_billing/recon_edit.html", 
                           account=acc, 
                           month=month, 
                           is_new=is_new,
                           metsoa_charges=metsoa_charges, 
                           metro_due=metro_due, 
                           owner_payment=owner_payment)

@billing_bp.route("/muni/recon/email", methods=["GET", "POST"])
@login_required
def muni_recon_email():
    from app.models.billing import BilMuniAccount, BilMeter, BilConsumption
    

    tenant_id = None
    if units and units[0].tenants:
        tenant_id = units[0].tenants[0].id

    if request.method == "POST":
        account_number = request.form.get("account_number")
        month = request.form.get("month")
        if account_number and month:
            return redirect(url_for("billing_bp.muni_recon_email", account_number=account_number, month=month))
            
    account_number = request.args.get("account_number")
    month = request.args.get("month")
    
    if not account_number or not month:
        accounts = BilMuniAccount.query.all()
        current_month = datetime.now().strftime("%Y-%m")
        return render_template("program_billing/recon_email_select.html", accounts=accounts, current_month=current_month)
        
    acc = BilMuniAccount.query.filter_by(account_number=account_number).first()
    if not acc:
        flash("Account not found.", "danger")
        return redirect(url_for("billing_bp.muni_recon_email"))
        
    meters = []
    if acc.water_meter_id:
        wm = BilMeter.query.get(acc.water_meter_id)
        if wm: meters.append(wm)
    if acc.elec_meter_id:
        em = BilMeter.query.get(acc.elec_meter_id)
        if em: meters.append(em)
        
    # Fallback to check if meters have the account number directly attached
    fallback_meters = BilMeter.query.filter_by(municipal_bill_number=account_number).all()
    for m in fallback_meters:
        if m not in meters:
            meters.append(m)
    
    body_lines = [
        "Good day,",
        "",
        f"Please find the meter readings for municipal account {account_number} for the billing period {month}:",
        ""
    ]
    
    if not meters:
        flash(f"No meters are linked to municipal account {account_number}.", "warning")
        return redirect(url_for("billing_bp.muni_recon_email"))
        
    for m in meters:
        body_lines.append(f"Meter Number: {m.meter_number}")
        
        cons = BilConsumption.query.filter_by(meter_id=m.id, month=month).first()
        if cons:
            if cons.new_date:
                body_lines.append(f"Reading Date: {cons.new_date.strftime('%Y-%m-%d')}")
            body_lines.append(f"Reading Value: {cons.new_read}")
        else:
            body_lines.append("Reading Date: _________________")
            body_lines.append("Reading Value: ________________")
            
        body_lines.append("")
        
    body_lines.append("Kind regards,")
    body_lines.append("Property Management")
    
    body_text = "\n".join(body_lines)
    
    subject = f"Meter Readings: Account {account_number} ({month})"
    # Default to account's electric email if available
    to_email = acc.muni_email or ""
    
    return render_template("program_billing/recon_email.html", 
                           account=acc, 
                           month=month, 
                           body_text=body_text,
                           subject=subject,
                           to_email=to_email)

@billing_bp.route("/billing/muni/recon/email/send", methods=["POST"])
@login_required
def muni_recon_email_send():
    from app.models.billing import BilMuniAccount, BilMeter, BilConsumption, BilMetroReadingLog
    from app.utils.mailer import send_email
    
    account_number = request.form.get("account_number")
    month = request.form.get("month")
    to_email = request.form.get("to_email")
    subject = request.form.get("subject")
    body_text = request.form.get("body_text")
    
    if not all([account_number, month, to_email, subject, body_text]):
        flash("All fields are required to send the email.", "danger")
        return redirect(url_for("billing_bp.muni_recon_email", account_number=account_number, month=month))
        
    acc = BilMuniAccount.query.filter_by(account_number=account_number).first()
    if not acc:
        flash("Account not found.", "danger")
        return redirect(url_for("billing_bp.muni_recon_email"))
        
    # Re-fetch meters to log the readings
    meters = []
    if acc.water_meter_id:
        wm = BilMeter.query.get(acc.water_meter_id)
        if wm: meters.append(wm)
    if acc.elec_meter_id:
        em = BilMeter.query.get(acc.elec_meter_id)
        if em: meters.append(em)
        
    fallback_meters = BilMeter.query.filter_by(municipal_bill_number=account_number).all()
    for m in fallback_meters:
        if m not in meters:
            meters.append(m)
            
    # Dispatch email
    try:
        success = send_email(subject=subject, recipients=[to_email], body=body_text)
        if not success:
            raise Exception("Mailer returned False")
    except Exception as e:
        flash(f"Failed to send email: {str(e)}", "danger")
        return redirect(url_for("billing_bp.muni_recon_email", account_number=account_number, month=month))
        
    # Log the readings
    records_logged = 0
    for m in meters:
        cons = BilConsumption.query.filter_by(meter_id=m.id, month=month).first()
        if cons:
            log_entry = BilMetroReadingLog(
                meter_number=m.meter_number,
                meter_id=m.id,
                account_number=account_number,
                reading_date=cons.new_date or datetime.now().date(),
                reading_value=cons.new_read,
                billing_period=month,
                metro_email=to_email
            )
            db.session.add(log_entry)
            records_logged += 1
            
    db.session.commit()
    flash(f"Email sent successfully to {to_email}. {records_logged} meter readings logged.", "success")
    return redirect(url_for("billing_bp.muni_recon_email"))

    # Only allow admins
    if not current_user.has_role('admin'):
        flash("Access denied", "danger")
        return redirect(url_for("public_bp.welcome"))


    tenant_id = None
    if units and units[0].tenants:
        tenant_id = units[0].tenants[0].id

    if request.method == "POST":
        # Create property + unit in one go
        prop = BilProperty(
            name=request.form["property_name"],
            address=request.form["address"],
            description=request.form.get("description"),
            manager_id=current_user.id,
            enrollment_id=_get_billing_enrollment_id(current_user.id)
        )
        db.session.add(prop)
        db.session.flush()

        unit = BilProperty(
            property_id=prop.id,
            unit_number=request.form["unit_number"]
        )
        db.session.add(unit)
        db.session.commit()

        flash("Property and unit added successfully!", "success")
        return redirect(url_for("billing_bp.learner_dashboard"))

    # Normal GET → show dashboard data
    data = get_dashboard_data()

    return render_template("program_billing/admin_dashboard.html", data=data)


def hash_password(password):
    return generate_password_hash(password)

def generate_month_list(n=6):
    today = datetime.today()
    months = []
    for i in range(n):
        month = (today.replace(day=1) - timedelta(days=30 * i)).strftime('%Y-%m')
        months.append(month)
    return sorted(set(months))

@billing_bp.route("/setup-property/<int:user_id>", methods=["GET", "POST"])
def setup_property(user_id):
    form = ManagerPropertyForm()
    manager = User.query.get_or_404(user_id)

    if form.validate_on_submit():
        property = BilProperty(
            name=form.property_name.data,
            address=form.address.data,
            unit_count=form.unit_count.data,
            manager_id=manager.id,
            property_type="external"
        )
        db.session.add(property)
        db.session.commit()
        return redirect(url_for("billing_bp.learner_dashboard"))
    return render_template("setup_property.html", form=form)

def get_available_months():
    rows = db.session.query(BilMeterReading.reading_date).all()
    months = sorted({r[0].strftime('%Y-%m') for r in rows if r[0]}, reverse=True)
    return months

def get_latest_month_for_tenant(tenant_id):
    months = get_available_months()
    return months[0] if months else None


def delete_all_consumption():
    """Deletes all records from bil_consumption table."""
    db.session.execute(text("DELETE FROM bil_consumption"))
    db.session.commit()
    print("🗑️ bil_consumption table cleared.")

def delete_all_consumption_copy():
    """Deletes all records from bil_consumption_copy table."""
    db.session.execute(text("DELETE FROM bil_consumption_copy"))
    db.session.commit()
    print("🗑️ bil_consumption_copy table cleared.")

def get_billing_months(num_months=6):
    """
    Returns a list of billing months in 'YYYY-MM' format,
    going back from the current month.
    """
    today = datetime.today()
    months = []

    for i in range(num_months):
        month = today.replace(day=1) - timedelta(days=i*30)
        months.append(month.strftime("%Y-%m"))

    # Optional: remove duplicates and sort descending
    return sorted(set(months), reverse=True)

@billing_bp.route('/admin/wipe_data', methods=['POST'])
@login_required
def wipe_tables():
    delete_all_consumption()
    delete_all_consumption_copy()
    flash("Consumption tables wiped clean.", "warning")
    return redirect(url_for("billing.admin_dashboard"))
 

# 🌐 Onboarding Route — stays in app/routes.py
@billing_bp.route('/onboard-property', methods=['GET', 'POST'])
@login_required
def onboard_property():
    if not (current_user.has_role('external_manager') or current_user.has_role('admin')):
        abort(403)

    form = PropertyForm()

    if form.validate_on_submit():
        new_property = BilProperty(
            name=form.name.data,
            location=form.location.data,
            type=form.type.data,
            is_external=True,
            managed_by_user_id=current_user.id
        )

        try:
            db.session.add(new_property)
            db.session.commit()
            flash("✅ Property onboarded successfully!", "success")
            return redirect(url_for("define_units", property_id=new_property.id))
        except Exception as e:
            try:
                db.session.rollback()
            except:
                pass
            print(f"💥 Property creation failed: {e}")
            flash("There was an error onboarding the property.", "danger")

    return render_template("onboard_property.html", form=form)

def generate_consumption_records_from_readings(month):
    """
    Regenerates consumption records for the given month by:
    - Deleting previous BilConsumption records for the month
    - Creating one consumption entry per meter based on consecutive readings
    """
    # 🚫 Clear existing records for the month
    BilConsumption.query.filter_by(month=month).delete()
    db.session.commit()

    records = []
    meters = BilMeter.query.all()

    if not meters:
        print("No meters found — skipping generation.")
        return records

    for meter in meters:
        # 🔍 Fetch all readings ordered by date for this meter
        readings = BilMeterReading.query.filter_by(meter_id=meter.id).order_by(BilMeterReading.reading_date).all()

        # ✅ Look for the first valid reading pair in the target month
        for i in range(1, len(readings)):
            prev = readings[i - 1]
            curr = readings[i]

            if curr.reading_date.strftime('%Y-%m') != month:
                continue

            try:
                days = (curr.reading_date - prev.reading_date).days
                usage = curr.reading_value - prev.reading_value

                record = BilConsumption(
                    meter_id=meter.id,
                    meter_number=meter.meter_number,
                    last_date=prev.reading_date,
                    new_date=curr.reading_date,
                    last_read=prev.reading_value,
                    new_read=curr.reading_value,
                    days=days,
                    consumption=int(round(usage)),
                    month=month
                )
                records.append(record)
            except Exception as e:
                print(f"⚠️ Error processing meter {meter.meter_number}: {e}")

            break  # Only generate one record per meter per month

    # 💾 Commit all new records
    if records:
        db.session.add_all(records)
        db.session.commit()
        print(f"✅ {len(records)} consumption records generated for {month}")
    else:
        print(f"⚠️ No valid consumption records generated for {month}")

    return records

def build_meter_charge_block(meter_id, month):
    rows = []

    # Get consumption
    consumption = BilConsumption.query.filter_by(
        meter_id=meter_id, month=month
    ).first()
    if not consumption:
        return rows  # Skip if no data

    # Tiered water blocks
    tariff_blocks = BilTariff.query.filter_by(
        utility_type="water"
    ).order_by(BilTariff.block_start.asc()).all()
    blocks = [{"start": t.block_start, "end": t.block_end, "rate": t.rate} for t in tariff_blocks]

    # Tiered logic
    def calculate_tiered_due(consumption_value, blocks):
        due = 0
        for block in blocks:
            if consumption_value > block["start"]:
                used = min(consumption_value, block["end"]) - block["start"]
                due += used * block["rate"]
        return round(due, 2)

    tiered_due = calculate_tiered_due(consumption.consumption, blocks)

    # Fixed charges
    fixed_charges = BilMeterFixedCharge.query.filter_by(
        meter_id=meter_id, month=month
    ).all()
    fixed_rows = []
    fixed_total = 0
    for fc in fixed_charges:
        fixed_rows.append({
            "description": fc.description,
            "amount": round(fc.amount, 2),
            "cons": fc.cons,
            "rate": fc.rate
        })
        fixed_total += fc.amount

    # WS Header
    rows.append({
        "meter_number": f"Water & Sanitation — {consumption.meter_number}",
        "last_date": "",
        "last_read": "",
        "new_date": "",
        "new_read": "",
        "days": "",
        "consumption": "",
        "rate": "",
        "due": ""
    })

    # Tiered row
    rows.append({
        "meter_number": f"{consumption.meter_number} — Tiered Water",
        "last_date": consumption.last_date.strftime('%Y-%m-%d'),
        "last_read": consumption.last_read,
        "new_date": consumption.new_date.strftime('%Y-%m-%d'),
        "new_read": consumption.new_read,
        "days": consumption.days,
        "consumption": consumption.consumption,
        "rate": "Tiered",
        "due": tiered_due
    })

    # Fixed charge rows
    for fr in fixed_rows:
        rows.append({
            "meter_number": f"{consumption.meter_number} — {fr['description']}",
            "last_date": "",
            "last_read": "",
            "new_date": "",
            "new_read": "",
            "days": "",
            "consumption": fr["cons"],
            "rate": fr["rate"],
            "due": fr["amount"]
        })

    # Subtotal
    rows.append({
        "meter_number": f"{consumption.meter_number} — WS Subtotal",
        "last_date": "",
        "last_read": "",
        "new_date": "",
        "new_read": "",
        "days": "",
        "consumption": "",
        "rate": "",
        "due": round(tiered_due + fixed_total, 2)
    })

    return rows

def calculate_ws_sd_total(meter_id, month):
    """
    Returns subtotal due for Water & Sanitation + SD charges for one meter.
    Used for Page 1 summary row.
    """

    # Fetch tiered water consumption
    consumption = BilConsumption.query.filter_by(
        meter_id=meter_id, month=month
    ).first()

    # Early exit if no data
    if not consumption:
        return {
            "meter_number": f"Meter ID {meter_id}",
            "due": 0
        }

    # Tiered WS tariff blocks
    tariff_blocks = BilTariff.query.filter_by(
        utility_type="water"
    ).order_by(BilTariff.block_start.asc()).all()

    blocks = [{"start": t.block_start, "end": t.block_end, "rate": t.rate} for t in tariff_blocks]

    # Calculate tiered water due
    def calculate_tiered_due(cons_value, blocks):
        due = 0
        for block in blocks:
            if cons_value > block["start"]:
                used = min(cons_value, block["end"]) - block["start"]
                due += used * block["rate"]
        return round(due, 2)

    tiered_due = calculate_tiered_due(consumption.consumption, blocks)

    # Fetch fixed charges
    fixed_charges = BilMeterFixedCharge.query.filter_by(
        meter_id=meter_id, month=month
    ).all()

    fixed_total = sum([fc.amount for fc in fixed_charges])

    # Total subtotal
    total = round(tiered_due + fixed_total, 2)

    return {
        "meter_number": consumption.meter_number,
        "due": total
    }

def build_ws_sd_rows_for_meter(meter_id, month, adjusted_consumption=None):
    # Step 1: Pull the meter object
    meter = BilMeter.query.get(meter_id)
    if not meter:
        return None  # Handle missing meter

    # Step 2: Pull consumption record using meter_id
    record = BilConsumption.query.filter_by(meter_id=meter_id, month=month).first()
    if not record:
        return None  # Handle missing data

    if adjusted_consumption is not None:
        consumption = adjusted_consumption
    else:
        consumption = record.consumption
    days = record.days

    # Step 3: Tiered WS breakdown
    ws_rows, ws_total = build_ws_breakdown(consumption, days, month, meter_id)

    # Step 4: Tiered SD breakdown
    sd_rows, sd_total = build_sd_breakdown(consumption, days, month, meter_id)

    # Step 5: Fixed & Sundry Charges
    fees = build_water_fee_block(record, month)

    ws_fixed_total = round(sum(row["due"] for row in fees["ws"]), 2)
    sd_fixed_total = round(sum(row["due"] for row in fees["sd"]), 2)

    return {
        "meter_number": meter.meter_number,
        "ws_cost": round(ws_total + ws_fixed_total, 2),
        "sd_cost": round(sd_total + sd_fixed_total, 2),
        "total": round(ws_total + sd_total + ws_fixed_total + sd_fixed_total, 2),
        "ws_breakdown": ws_rows + fees["ws"],
        "sd_breakdown": sd_rows + fees["sd"]
    }

def build_ws_sd_subtotals():
    """
    Returns structured WS, SD, and total rows for Page 1.
    Total returned separately for metro accumulation.
    """
    # Mock values for demo
    ws = round(74.04, 2)
    sd = round(53.40, 2)
    total = round(ws + sd, 2)

    rows = [
        {
            "meter_number":"WS Sub-Total",
            "last_date": "", "new_date": "",
            "last_read": "", "new_read": "", "days": "",
            "rate": "", "consumption": "",
            "due": ws
        },
        {
            "meter_number":"SD Sub-Total",
            "last_date": "", "new_date": "",
            "last_read": "", "new_read": "", "days": "",
            "rate": "", "consumption": "",
            "due": sd
        },
        {
            "meter_number":"Cost of Water",
            "last_date": "", "new_date": "",
            "last_read": "", "new_read": "", "days": "",
            "rate": "", "consumption": "",
            "due": total
        }
    ]

    return rows, ws, sd, total
    
def build_ws_breakdown(consumption, days, month, meter_id):
    ws_tiers = get_tariff_for_ws(meter_id, month)  # Pull from DB or tariff config

    breakdown = []
    total_due = 0.0
    remaining = consumption

    for tier in ws_tiers:
        max_alloc = round(tier["multiplier"] * days, 3)
        used = min(remaining, max_alloc)
        due = round(used * tier["rate"], 2)

        breakdown.append({
            "desc": tier.get("label", f"WS Tier @ R{tier['rate']}"),
            "cons": used,
            "rate": tier["rate"],
            "due": due
        })

        total_due += due
        remaining -= used
        if remaining <= 0:
            break

    # Add Surcharge universally
    surcharge_rate = 1.48
    surcharge_due = round(consumption * surcharge_rate, 2)
    breakdown.append({
        "desc": "Surcharge",
        "cons": consumption,
        "rate": surcharge_rate,
        "due": surcharge_due
    })
    total_due += surcharge_due

    return breakdown, round(total_due, 2)

def build_sd_breakdown(consumption, days, month, meter_id):
    sd_tiers = get_tariff_for_sd(meter_id, month)  # Pull SD structure from DB

    breakdown = []
    total_due = 0.0
    remaining = consumption

    for tier in sd_tiers:
        max_alloc = round(tier["multiplier"] * days, 3)
        used = min(remaining, max_alloc)
        due = round(used * tier["rate"], 2)

        breakdown.append({
            "desc": tier.get("label", f"SD Tier @ R{tier['rate']}"),
            "cons": used,
            "rate": tier["rate"],
            "due": due
        })

        total_due += due
        remaining -= used
        if remaining <= 0:
            break

    # Add Surcharge universally
    surcharge_rate = 1.48
    surcharge_due = round(consumption * surcharge_rate, 2)
    breakdown.append({
        "desc": "Surcharge",
        "cons": consumption,
        "rate": surcharge_rate,
        "due": surcharge_due
    })
    total_due += surcharge_due

    return breakdown, round(total_due, 2)

def build_water_fee_block(r, month):
    meter_number = r.meter_number
    consumption = r.consumption

    ws_fees = []
    sd_fees = []
    
    # Query database for Fixed WS/SD charges
    db_fixed_ws = BilTariff.query.filter_by(utility_type='water_fixed').all()
    if db_fixed_ws:
        for f in db_fixed_ws:
            ws_fees.append({"desc": f.description or f.code, "cons": 0.00, "rate": 0.00, "due": f.rate})
    else:
        # Fallback
        ws_fees.append({"desc": "Water Loss Charge", "cons": 0.00, "rate": 0.00, "due": 19.30})
        
    db_fixed_sd = BilTariff.query.filter_by(utility_type='sewerage_fixed').all()
    if db_fixed_sd:
        for f in db_fixed_sd:
            sd_fees.append({"desc": f.description or f.code, "cons": 0.00, "rate": 0.00, "due": f.rate})
            
    if meter_number == "AGN489" and not db_fixed_ws and not db_fixed_sd:
        # Legacy fallback
        ws_fees.append({"desc": "Monthly Management Fee", "cons": 6, "rate": 24.09, "due": 144.54})
        sd_fees.append({"desc": "Refuse Bins", "cons": 0, "rate": 0, "due": 206.93})
        
    return {"ws": ws_fees, "sd": sd_fees}

def build_raw_water_row(meter_id, month):
    """
    Returns a single row for the given water meter using only consumption table data.
    'Rate' and 'Due' fields are intentionally left blank.
    Used for layout alignment with electrical block on Page 1.
    """

    cons = BilConsumption.query.filter_by(
        meter_id=meter_id, month=month
    ).first()
    if not cons:
        return []

    return [{
        "meter_number": str(cons.meter_id),
        "code": cons.meter_number,
        "last_date": cons.last_date.strftime("%Y/%m/%d"),
        "last_read": cons.last_read,
        "new_date": cons.new_date.strftime("%Y/%m/%d"),
        "new_read": cons.new_read,
        "days": cons.days,
        "consumption": cons.consumption,
        "rate": "",
        "due": ""
    }]

def get_all_property_meters(property_id):
    from app.models.billing import BilProperty, BilMuniAccount, BilMeter
    all_meters = BilMeter.query.filter_by(property_id=property_id).all()
        
    muni_accounts = BilMuniAccount.query.filter_by(property_id=property_id).all()
    muni_acc_numbers = [acc.account_number for acc in muni_accounts if acc.account_number]
    if muni_acc_numbers:
        muni_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(muni_acc_numbers)).all()
        for m in muni_meters:
            if m not in all_meters:
                all_meters.append(m)
                
    for acc in muni_accounts:
        if acc.water_meter and acc.water_meter not in all_meters:
            all_meters.append(acc.water_meter)
        if acc.elec_meter and acc.elec_meter not in all_meters:
            all_meters.append(acc.elec_meter)
            
    return all_meters

def build_electrical_rows(property_id, month, is_exception=False, filter_meter_ids=None):
    rows = []
    total_due = 0

    meters = get_all_property_meters(property_id)
    if is_exception:
        active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() != 'active']
    else:
        active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() == 'active']
    meter_types = {m.id: m.utility_type for m in meters}
    
    # Pre-calculate sub-meter deductions for bulk meters
    records = BilConsumption.query.filter_by(month=month).all()
    bulk_deductions = {}
    for r in records:
        m = next((meter for meter in meters if meter.id == r.meter_id), None)
        if m and m.parent_meter_id:
            bulk_deductions[m.parent_meter_id] = bulk_deductions.get(m.parent_meter_id, 0) + r.consumption
            
    linked_meter_ids = [m.id for m in meters]
    tenant_records = [r for r in records if r.meter_id in linked_meter_ids]

    rate_obj = BilTariff.query.filter_by(utility_type="electricity").first()
    rate = rate_obj.rate if rate_obj else 0

    for r in tenant_records:
        if r.meter_id not in active_meter_ids:
            continue
        if filter_meter_ids is not None and r.meter_id not in filter_meter_ids:
            continue
        if filter_meter_ids is not None and r.meter_id not in filter_meter_ids:
            continue
            
        if (meter_types.get(r.meter_id) or "").lower() in ["electricity", "electrical"]:
            adj_cons = r.consumption - bulk_deductions.get(r.meter_id, 0)
            adj_cons = max(0, adj_cons)
            
            due = round(adj_cons * rate, 2)
            total_due += due
            rows.append({
                "meter_number": r.meter_number,
                "last_date": r.last_date.strftime('%Y/%m/%d'),
                "new_date": r.new_date.strftime('%Y/%m/%d'),
                "last_read": r.last_read,
                "new_read": r.new_read,
                "days": r.days,
                "avg": 0, # Placeholder
                "consumption": adj_cons,
                "rate": rate,
                "due": due
            })
    return rows, round(total_due, 2)

def get_tariff_for_ws(meter_id, month):
    db_tariffs = BilTariff.query.filter_by(utility_type='water').order_by(BilTariff.block_start).all()
    if db_tariffs:
        return [
            {
                "start": t.block_start,
                "label": t.description or f"{t.block_start}-{t.block_end}L/day",
                "multiplier": t.reduction_factor,
                "rate": t.rate
            } for t in db_tariffs
        ]
        
    # Fallback: replace with DB query
    return [
        {"start": 0, "label": "0L-200L/day", "multiplier": 0.200, "rate": 39.39},
        {"start": 201, "label": "201L-833L/day", "multiplier": 0.633, "rate": 46.70},
        {"start": 834, "label": "834L-1KL/day", "multiplier": 0.167, "rate": 62.17},
        {"start": 1001, "label": "1KL-1.5KL/day", "multiplier": 0.500, "rate": 95.91},
        {"start": 1501, "label": ">1.5KL/day", "multiplier": 999.0, "rate": 105.39}
    ]

def get_tariff_for_sd(meter_id, month):
    db_tariffs = BilTariff.query.filter_by(utility_type='sewerage').order_by(BilTariff.block_start).all()
    if db_tariffs:
        return [
            {
                "start": t.block_start,
                "label": t.description or f"{t.block_start}-{t.block_end}L/day",
                "multiplier": t.reduction_factor,
                "rate": t.rate
            } for t in db_tariffs
        ]
        
    # Fallback
    return [
        {"start": 0, "label": "0L-200L/29Days", "multiplier": 0.200, "rate": 5.45},
        {"start": 201, "label": "201L-833L/29Days", "multiplier": 0.633, "rate": 9.20},
        {"start": 834, "label": "833L-1KL/29Days", "multiplier": 0.167, "rate": 17.54},
        {"start": 1001, "label": "1KL-1,5KL/29Days", "multiplier": 999.0, "rate": 27.38}
    ]

def build_water_rows(property_id, month, is_exception=False, filter_meter_ids=None):
    water_meters = []
    total_water_due = 0

    meters = get_all_property_meters(property_id)
    if is_exception:
        active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() != 'active']
    else:
        active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() == 'active']
    meter_types = {m.id: m.utility_type for m in meters}
    
    # Pre-calculate sub-meter deductions for bulk meters
    records = BilConsumption.query.filter_by(month=month).all()
    bulk_deductions = {}
    for r in records:
        m = next((meter for meter in meters if meter.id == r.meter_id), None)
        if m and m.parent_meter_id:
            bulk_deductions[m.parent_meter_id] = bulk_deductions.get(m.parent_meter_id, 0) + r.consumption
            
    linked_meter_ids = [m.id for m in meters]
    tenant_records = [r for r in records if r.meter_id in linked_meter_ids]

    for r in tenant_records:
        if r.meter_id not in active_meter_ids:
            continue
        if filter_meter_ids is not None and r.meter_id not in filter_meter_ids:
            continue
        if filter_meter_ids is not None and r.meter_id not in filter_meter_ids:
            continue
            
        if (meter_types.get(r.meter_id) or "").lower() == "water":
            meter_id = r.meter_id
            adj_cons = r.consumption - bulk_deductions.get(r.meter_id, 0)
            adj_cons = max(0, adj_cons)
            
            summary = {
                "meter_number": r.meter_number,
                "last_date": r.last_date.strftime('%Y/%m/%d'),
                "new_date": r.new_date.strftime('%Y/%m/%d'),
                "last_read": r.last_read,
                "new_read": r.new_read,
                "days": f"{r.days} KL/day",
                "avg": round(adj_cons / r.days, 1) if r.days else 0,
                "consumption": adj_cons,
                "rate": "",
                "due": ""
            }
            
            details = build_ws_sd_rows_for_meter(meter_id, month, adjusted_consumption=adj_cons)
            if details:
                details["summary"] = summary
                water_meters.append(details)
                total_water_due += details["total"]

    return water_meters, round(total_water_due, 2)

def add_water_subtotals_to_page1(rows, meter_number, cost_block):
    """
    Appends WS, SD, and total water cost rows to Page 1 layout.
    """
    rows.extend([
        {
            "meter_number": "",
            "code": f"W&S Cost For #{meter_number}",
            "last_date": "", "last_read": "",
            "new_date": "", "new_read": "",
            "days": "", "consumption": "",
            "rate": "", "due": cost_block["ws_total"]
        },
        {
            "meter_number": "",
            "code": f"S & D Cost #{meter_number}",
            "last_date": "", "last_read": "",
            "new_date": "", "new_read": "",
            "days": "", "consumption": "",
            "rate": "", "due": cost_block["sd_total"]
        },
        {
            "meter_number": "",
            "code": f"Total Water Cost For #{meter_number}",
            "last_date": "", "last_read": "",
            "new_date": "", "new_read": "",
            "days": "", "consumption": "",
            "rate": "", "due": cost_block["total"]
        }
    ])

def build_ws_sd_cost_block(meter_id, month):
    """
    For a given meter, returns detailed WS and SD breakdown rows for Page 2.
    Each row includes: desc, cons, rate, and due.
    Totals are calculated per section and summed as meter total.
    """
    ws_rows = []
    sd_rows = []
    ws_total = 0
    sd_total = 0

    # Pull meter and consumption
    meter = BilMeter.query.get(meter_id)
    cons = BilConsumption.query.filter_by(meter_id=meter_id, month=month).first()
    if not meter or not cons:
        return None

    consumption = cons.consumption
    days = cons.days

    # 🔹 WS Tiered Tariff Rows
    ws_tiers = BilTariff.query.filter_by(utility_type="water").order_by(BilTariff.block_start.asc()).all()
    for tier in ws_tiers:
        block_max = tier.block_end if tier.block_end is not None else float('inf')
        slice_volume = max(0, min(consumption, block_max) - tier.block_start)
        if slice_volume <= 0:
            continue

        charge = round(slice_volume * tier.rate, 2)
        ws_total += charge

        ws_rows.append({
            "desc": f"WS Tier {tier.block_start}–{tier.block_end or '∞'}L / {days} Days",
            "cons": slice_volume,
            "rate": round(tier.rate, 2),
            "due": charge
        })

    # 🔹 SD Tiered Tariff Rows
    sd_tiers = BilTariff.query.filter_by(utility_type="sanitation").order_by(BilTariff.block_start.asc()).all()
    for tier in sd_tiers:
        block_max = tier.block_end if tier.block_end is not None else float('inf')
        slice_volume = max(0, min(consumption, block_max) - tier.block_start)
        if slice_volume <= 0:
            continue

        charge = round(slice_volume * tier.rate, 2)
        sd_total += charge

        sd_rows.append({
            "desc": f"SD Tier {tier.block_start}–{tier.block_end or '∞'}L / {days} Days",
            "cons": slice_volume,
            "rate": round(tier.rate, 2),
            "due": charge
        })

    # 🧾 Combine all subtotals
    total = round(ws_total + sd_total, 2)

    return {
        "meter_number": meter.meter_number,
        "code": getattr(meter, "code", meter.meter_number),
        "ws_rows": ws_rows,
        "sd_rows": sd_rows,
        "ws_total": round(ws_total, 2),
        "sd_total": round(sd_total, 2),
        "total": total
    }

def get_fixed_charge_for_meter(meter_id, month):
    # Placeholder value; link to DB table if needed
    return 45.00

def get_water_loss_levy(meter_id, month):
    return 17.12  # Flat rate per meter

def get_sundry_ws(consumption):
    return round(0.48 * consumption, 2)  # e.g., 48c per KL

def get_sundry_sd(consumption):
    return round(0.32 * consumption, 2)  # e.g., 32c per KL


from datetime import date
from app.models.billing import BilTenantLedger

def _auto_post_to_ledger(tenant_id, month, grand_total, tenant, elec_rows=None):
    from datetime import date, datetime
    # Determine dates
    try:
        y, m = map(int, month.split('-'))
        base_date = date(y, m, 1)
    except:
        base_date = date.today()
        
    metsoa_date = base_date
    if elec_rows:
        try:
            last_date_str = elec_rows[-1].get('new_date')
            if last_date_str:
                metsoa_date = datetime.strptime(last_date_str, "%Y/%m/%d").date()
        except Exception:
            pass

    # 1. Post Arrears (Opening Balance)
    arrears_amount = 0
    if tenant.leases:
        arrears_amount = tenant.leases[0].tenant_arrears_total or 0
    if arrears_amount > 0:
        arrears_entry = BilTenantLedger.query.filter_by(tenant_id=tenant_id, month=month, ref="ARREARS-AUTO").first()
        if arrears_entry:
            arrears_entry.amount = arrears_amount
            arrears_entry.description = "Opening Balance"
            arrears_entry.txn_date = base_date
        else:
            arrears_entry = BilTenantLedger(
                tenant_id=tenant_id,
                month=month,
                txn_date=base_date,
                description="Opening Balance",
                kind="charge",
                amount=arrears_amount,
                ref="ARREARS-AUTO"
            )
            db.session.add(arrears_entry)

    # 2. Post Rent
    rent_amount = 0
    if tenant.leases:
        rent_amount = tenant.leases[0].rent_amount or 0
    if rent_amount > 0:
        rent_entry = BilTenantLedger.query.filter_by(tenant_id=tenant_id, month=month, ref="RENT-AUTO").first()
        if rent_entry:
            rent_entry.amount = rent_amount
            rent_entry.txn_date = base_date
        else:
            rent_entry = BilTenantLedger(
                tenant_id=tenant_id,
                month=month,
                txn_date=base_date,
                description="Monthly Rent",
                kind="charge",
                amount=rent_amount,
                ref="RENT-AUTO"
            )
            db.session.add(rent_entry)
            
    # 3. Post METSOA Total
    metsoa_entry = BilTenantLedger.query.filter_by(tenant_id=tenant_id, month=month, ref="METSOA-AUTO").first()
    if metsoa_entry:
        metsoa_entry.amount = grand_total
        metsoa_entry.txn_date = metsoa_date
    else:
        metsoa_entry = BilTenantLedger(
            tenant_id=tenant_id,
            month=month,
            txn_date=metsoa_date,
            description="Utilities (METSOA)",
            kind="charge",
            amount=grand_total,
            ref="METSOA-AUTO"
        )
        db.session.add(metsoa_entry)

    db.session.commit()


@billing_bp.route("/billing/utilities/<int:property_id>/exception_metsoa/<month>")
@login_required
def exception_metsoa(property_id, month):
    from app.models.billing import BilProperty
    
    prop = BilProperty.query.get_or_404(property_id)

    if prop:
        manager_id = prop.manager_id
        from app.models.billing import BilStatementPayment
        payment = BilStatementPayment.query.filter_by(manager_id=manager_id, month=month).first()
        if not payment or payment.amount_paid_cents <= 0:
            if current_user.id == manager_id:
                flash(f"Please unlock statements for {month} before viewing or generating PDFs.", "warning")
                return redirect(url_for('billing_bp.billing_checkout', month=month))
            elif current_user.has_role('admin'):
                pass
            else:
                flash("Your manager has not unlocked statements for this month yet.", "danger")
                return redirect(url_for("public_bp.welcome"))

    elec_rows, elec_total = build_electrical_rows(prop.id, month, is_exception=True)
    water_meters, water_total = build_water_rows(prop.id, month, is_exception=True)
    
    grand_total = round(elec_total + water_total, 2)
    
    data = {
        "tenant": None,
        "property": prop,
        "month": month,
        "electricity": {
            "rows": elec_rows,
            "subtotal": elec_total
        },
        "water": {
            "meters": water_meters,
            "total": water_total
        },
        "grand_total": grand_total
    }

    return render_template("program_billing/exception_metsoa.html", **data)

@billing_bp.route("/billing/utilities/<int:property_id>/exception_metsoa/<month>/email", methods=["POST"])
@login_required
def email_exception_metsoa(property_id, month):
    from app.models.billing import BilProperty
    from flask import request
    from app.utils.mailer import send_pdf_email
    import tempfile
    import os
    
    data_req = request.get_json()
    email = data_req.get("email") if data_req else None
    if not email:
        return {"success": False, "error": "Email address is required"}, 400
        
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        return {"success": False, "error": "Unauthorized"}, 403

    elec_rows, elec_total = build_electrical_rows(prop.id, month, is_exception=True)
    water_meters, water_total = build_water_rows(prop.id, month, is_exception=True)
    
    grand_total = round(elec_total + water_total, 2)
    
    data = {
        "tenant": None,
        "property": prop,
        "month": month,
        "electricity": {
            "rows": elec_rows,
            "subtotal": elec_total
        },
        "water": {
            "meters": water_meters,
            "total": water_total
        },
        "grand_total": grand_total
    }

    html_string = render_template("program_billing/exception_metsoa.html", **data)
    
    # Hide the action buttons in the PDF
    html_string = html_string.replace('class="print:hidden', 'style="display:none;"')
    
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        pdf_path = tmp.name

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(html_string, wait_until="networkidle")
            page.pdf(
                path=pdf_path, 
                format="A4", 
                print_background=True,
                display_header_footer=True,
                header_template="<span></span>",
                footer_template="<div style='width: 100%; text-align: center; font-size: 10px; color: #6b7280; padding-bottom: 10px;'>Page <span class='pageNumber'></span> of <span class='totalPages'></span></div>",
                margin={"top": "30px", "bottom": "40px", "left": "10px", "right": "10px"}
            )
            browser.close()
            
        with open(pdf_path, 'rb') as f_pdf:
            pdf_bytes = f_pdf.read()
            
        os.remove(pdf_path)
            
        subject = f"Exception METSOA Review - {prop.name} - {month}"
        body = f"Hello,\n\nPlease find the Exception METSOA statement for {prop.name} for the billing month of {month} attached as a PDF.\n\nRegards,\nAIT Platform"
        
        success = send_pdf_email(email, subject, body, pdf_bytes, filename=f"{month}-Exception-MetSoa-{prop.name}.pdf")
        if success:
            return {"success": True}
        else:
            return {"success": False, "error": "Mailer returned false."}
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
        return {"success": False, "error": str(e)}

@billing_bp.route('/metsoa/<int:property_id>/<month>', methods=['GET'])
@login_required
def metsoa(property_id, month):
    from app.models.billing import BilProperty
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    manager_id = prop.manager_id
    from app.models.billing import BilStatementPayment
    payment = BilStatementPayment.query.filter_by(manager_id=manager_id, month=month).first()
    if not payment or payment.amount_paid_cents <= 0:
        if current_user.id == manager_id:
            flash(f"Please unlock statements for {month} before viewing or generating PDFs.", "warning")
            return redirect(url_for('billing_bp.billing_checkout', month=month))
        elif current_user.has_role('admin'):
            flash(f"Notice: Manager has not paid for {month} statements.", "info")
        else:
            flash("Your manager has not unlocked statements for this month yet.", "danger")
            return redirect(url_for("public_bp.welcome"))

    elec_rows, elec_total = build_electrical_rows(prop.id, month)
    water_meters, water_total = build_water_rows(prop.id, month)
    
    grand_total = round(elec_total + water_total, 2)
    
    data = {
        "tenant": None,
        "property": prop,
        "month": month,
        "electricity": {
            "rows": elec_rows,
            "subtotal": elec_total
        },
        "water": {
            "meters": water_meters,
            "total": water_total
        },
        "grand_total": grand_total
    }
    return render_template("program_billing/metsoa.html", **data)

@billing_bp.route('/tenant/<int:tenant_id>')
def tenant_mapping(tenant_id):
    tenant_list = get_mapped_tenant_by_id(tenant_id)
    tenant = tenant_list[0] if tenant_list else None
    return render_template("tenant_mapping.html", tenant=tenant)

def static_page(name):
    return render_template(f"billing/{name}.html")



@billing_bp.route("/healthcheck")
def health_check():
    diagnostics = {}

    # DB Check
    try:
        result = db.session.execute(text("SELECT 1")).scalar()
        diagnostics["database"] = {"status": "✅ healthy", "result": result}
    except Exception as e:
        diagnostics["database"] = {"status": "❌ error", "details": str(e)}

    # Uptime Check
    diagnostics["uptime"] = {"status": "✅ running", "timestamp": time.time()}

    # Optional: Cache / External Service Checks (e.g. Redis, Auth, API)
    # diagnostics["cache"] = cache_check()
    # diagnostics["auth_service"] = auth_ping()

    return jsonify(diagnostics), 200 if diagnostics["database"]["status"] == "✅ healthy" else 500
    
#from helpers.tenants import get_tenant_info

#@billing_bp.route('/debug-tenant/<int:tenant_id>')
#def debug_tenant(tenant_id):
    #info = get_tenant_info(tenant_id)
    #return jsonify(info)

import json
from app.models.billing import BilLease

@billing_bp.route("/api/parse_bill", methods=["POST"])
@login_required
def parse_bill_api():
    if 'bill_file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
        
    file = request.files['bill_file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
        
    try:
        from google import genai
        from google.genai import types
        import os
        import json
        from dotenv import load_dotenv
        from flask import current_app
        
        dotenv_path = os.path.join(current_app.root_path, '..', '.env')
        load_dotenv(dotenv_path, override=True)
        
        api_key = os.environ.get("GEMINI_API_KEY") or current_app.config.get("GEMINI_API_KEY")
        
        # Bulletproof fallback: manually parse .env if still missing
        if not api_key:
            try:
                with open(dotenv_path, 'r', encoding='utf-8') as ef:
                    for line in ef:
                        if line.startswith('GEMINI_API_KEY='):
                            api_key = line.split('=', 1)[1].strip().strip('"').strip("'")
                            break
            except Exception:
                pass

        if not api_key:
            return jsonify({"error": "GEMINI_API_KEY is not configured"}), 500
            
        client = genai.Client(api_key=api_key)
        
        file_bytes = file.read()
        mime_type = file.content_type
        
        
        
        prompt = '''
        Analyze this municipality bill (typically Ethekwini/Durban format) and extract the following information.
        Return the result strictly as a valid JSON object with the following keys:
        - "property_name": The name of the property or owner (string)
        - "address": The full address of the property (string)
        - "metro_account_no": The municipal account number (string). Look for ANY 11-digit number (like 83187242565) anywhere on the document. If you cannot find any 11-digit number, return the exact string "NOT FOUND".
        - "water_meters": An array of water meter numbers (array of strings). Ethekwini water meters often end with a letter (e.g. 'W' or 'S') or are just numeric. Look under the 'Meter Readings' or 'Water' sections. YOU MUST NOT SKIP ANY WATER METERS.
        - "electricity_meters": An array of electricity meter numbers (array of strings). Ethekwini electricity meters often end with a letter (e.g. 'E', 'S', 'X') or are numeric. Look under 'Electricity' or 'Meter Readings'. YOU MUST NOT SKIP ANY ELECTRICITY METERS.
        - "muni_email": The contact email address for the municipality (string)
        - "has_rates": A boolean (true or false), true if the bill includes a property rates charge (sometimes called property tax or assessment rates)
        - "rates_amount": The monetary amount charged for property rates, excluding currency symbols (string or number)
        - "amount_due": The total initial amount due or balance on the bill (string or number)
        - "readings": An array of objects, where each object represents a meter reading line and MUST have "meter_number" (string) and "utility_type" ("water" or "electricity"). Extract every single meter reading line you can find on the bill.
        
        If a field is not found, return an empty string or empty array for that key. Do not include markdown formatting like ```json.
        '''
        
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[types.Part.from_bytes(data=file_bytes, mime_type=mime_type), prompt],
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        
        text_response = response.text.strip()
        if text_response.startswith('```json'):
            text_response = text_response[7:]
        if text_response.endswith('```'):
            text_response = text_response[:-3]
            
        data = json.loads(text_response.strip())
        
        print("\n=== AI EXTRACTED DATA ===")
        print(json.dumps(data, indent=2))
        print("=========================\n")
        
        return jsonify(data)
        
    except Exception as e:
        return jsonify({"error": f"Failed to parse bill: {str(e)}"}), 500

def build_wizard_data_from_db(property_id):
    from app.models.billing import BilMuniAccount, BilMeter, RefMuniOwner
    wizardData = {
        'accounts': [],
        'bulkWater': [],
        'bulkElec': [],
        'subWater': [],
        'subElec': [],
        'exceptions': [],
        'mapping': [],
        'arrears': [],
        'rates': [],
        'arrangements': [],
        'owners': [],
        'propertyMap': {}
    }
    
    accounts = BilMuniAccount.query.filter_by(property_id=property_id).order_by(BilMuniAccount.id).all()
    if not accounts:
        return None
        
    acc_map = {}
    bulk_acc_num = None
    
    for i, acc in enumerate(accounts):
        acc_id_str = f"acc_{i}"
        acc_map[acc.account_number] = acc_id_str
        if acc.is_bulk_account:
            bulk_acc_num = acc.account_number
            
        wizardData['accounts'].append({
            'id': acc_id_str,
            'number': acc.account_number,
            'owner': '', # Handled in owners section
            'isBulk': acc.is_bulk_account
        })
        
        # Arrears
        if getattr(acc, 'arrears_amount', None) or getattr(acc, 'arrears_date', None):
            wizardData['arrears'].append({
                'account_id': acc_id_str,
                'amount': acc.arrears_amount or 0.0,
                'date': acc.arrears_date.strftime("%Y-%m-%d") if acc.arrears_date else '',
                'charge_to': acc.arrears_charge_to or 'owner'
            })
            
        # Rates
        if acc.rates_date:
            wizardData['rates'].append({
                'account_id': acc_id_str,
                'amount': acc.rates_amount or 0.0,
                'date': acc.rates_date.strftime("%Y-%m-%d") if acc.rates_date else '',
                'charge_to': acc.rates_charge_to or 'owner',
                'reference': acc.rates_reference or '',
                'erf_details': acc.rates_erf_details or '',
                'property_category': acc.rates_property_category or '',
                'market_value': acc.rates_market_value or 0.0,
                'rateable_value': acc.rates_rateable_value or 0.0,
                'general_randage': acc.rates_general_randage or 0.0,
                'sra_randage': acc.rates_sra_randage or 0.0,
                'deferred': acc.rates_deferred or 0.0,
                'sra_monthly': acc.rates_sra_monthly or 0.0,
                'general_monthly': acc.rates_general_monthly or 0.0
            })
            
        # Arrangements
        if acc.arrangement_date:
            wizardData['arrangements'].append({
                'account_id': acc_id_str,
                'charge_to': acc.arrangement_charge_to or 'owner',
                'contract_number': acc.ca_contract_number or '',
                'agreement_amount': acc.ca_agreement_amount or 0.0,
                'installments_raised': acc.ca_installments_raised or 0.0,
                'installment_amount': acc.ca_installment_amount or 0.0,
                'amount_owing': acc.ca_amount_owing or 0.0,
                'remaining_periods': acc.ca_remaining_periods or 0,
                'date': acc.arrangement_date.strftime("%Y-%m-%d") if acc.arrangement_date else ''
            })
            
        # Owners
        if acc.owner_id:
            owner = db.session.get(RefMuniOwner, acc.owner_id)
            if owner:
                wizardData['owners'].append({
                    'account_id': acc_id_str,
                    'name': owner.name or '',
                    'email': acc.muni_email or '',
                    'address': acc.owner_address or ''
                })

    acc_nums = [a.account_number for a in accounts if a.account_number]
    meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(acc_nums)).all()
    
    bw_count = 0
    be_count = 0
    sw_count = 0
    se_count = 0
    
    mapping_dict = {acc['id']: {'account_id': acc['id'], 'water': '', 'elec': ''} for acc in wizardData['accounts'] if not acc['isBulk']}
    
    db_to_ui_meter = {}
    
    for m in meters:
        if m.status == 'stolen':
            continue
            
        u = (m.utility_type or '').lower()
        is_bulk = (m.municipal_bill_number == bulk_acc_num)
        
        m_id = ""
        if 'water' in u:
            if is_bulk:
                m_id = f"bulk-water_{bw_count}"
                wizardData['bulkWater'].append({'id': m_id, 'number': m.meter_number})
                bw_count += 1
            else:
                m_id = f"sub-water_{sw_count}"
                wizardData['subWater'].append({'id': m_id, 'number': m.meter_number})
                sw_count += 1
        else:
            if is_bulk:
                m_id = f"bulk-elec_{be_count}"
                wizardData['bulkElec'].append({'id': m_id, 'number': m.meter_number})
                be_count += 1
            else:
                m_id = f"sub-elec_{se_count}"
                wizardData['subElec'].append({'id': m_id, 'number': m.meter_number})
                se_count += 1
                
        db_to_ui_meter[m.id] = m_id
        
        # Mapping
        if not is_bulk and m.municipal_bill_number in acc_map:
            acc_id = acc_map[m.municipal_bill_number]
            if acc_id in mapping_dict:
                if 'water' in u:
                    mapping_dict[acc_id]['water'] = m_id
                else:
                    mapping_dict[acc_id]['elec'] = m_id

    # Now add exceptions
    for m in meters:
        if m.status == 'stolen':
            rep_m = next((rm for rm in meters if rm.replacement_for_meter_id == m.id), None)
            if rep_m and rep_m.id in db_to_ui_meter:
                wizardData['exceptions'].append({
                    'stolen_num': m.meter_number,
                    'replacement_id': db_to_ui_meter[rep_m.id],
                    'date_stolen': m.date_stolen.strftime('%Y-%m-%d') if m.date_stolen else '',
                    'date_replaced': m.date_replaced.strftime('%Y-%m-%d') if m.date_replaced else ''
                })

    wizardData['mapping'] = list(mapping_dict.values())

    wizardData['propertyMap'] = {
        'accounts': len(accounts),
        'water': bw_count + sw_count,
        'elec': be_count + se_count,
        'bulkWater': bw_count > 0,
        'bulkElec': be_count > 0,
        'owners': len(set([o['name'] for o in wizardData['owners'] if o['name']])) or 1,
        'addresses': len(set([o['address'] for o in wizardData['owners'] if o['address']])) or 1
    }
    
    # Extract Initial Readings
    from app.models.billing import BilMeterReading
    wizardData['initialReadings'] = []
    
    active_meter_ids = [m.id for m in meters if m.status != 'stolen']
    if active_meter_ids:
        readings = BilMeterReading.query.filter(BilMeterReading.meter_id.in_(active_meter_ids)).all()
        meter_id_to_num = {m.id: m.meter_number for m in meters if m.status != 'stolen'}
        
        meter_readings = {}
        for r in readings:
            if r.meter_id in meter_id_to_num:
                m_num = meter_id_to_num[r.meter_id]
                # Keep earliest reading
                if m_num not in meter_readings or (r.reading_date and meter_readings[m_num]['date_obj'] and r.reading_date < meter_readings[m_num]['date_obj']):
                    meter_readings[m_num] = {
                        'value': r.reading_value,
                        'date': r.reading_date.strftime('%Y-%m-%d') if r.reading_date else '',
                        'date_obj': r.reading_date
                    }
                    
        for m_num, rd in meter_readings.items():
            wizardData['initialReadings'].append({
                'meter_number': m_num,
                'value': rd['value'],
                'date': rd['date']
            })
    
    return wizardData

@billing_bp.route("/billing/setup", methods=["GET"])
@login_required
def setup_wizard():
    property_id = request.args.get('property_id')
    if not property_id:
        flash("Missing property ID", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    from app.models import BilProperty
    property = BilProperty.query.get_or_404(property_id)
    if property.manager_id != current_user.id:
        from flask import abort
        abort(403)
        
    from app.models.billing import BilArchitectureDraft, BilMuniAccount
    import json
    
    # Check if accounts exist for this property (meaning it was already saved/finalised)
    accounts_exist = BilMuniAccount.query.filter_by(property_id=prop.id).first()
    
    if accounts_exist:
        # Rehydrate dynamically from SQL tables
        draft_json = build_wizard_data_from_db(property.id)
    else:
        # Unfinalised property, fall back to draft JSON
        draft_record = BilArchitectureDraft.query.filter_by(property_id=prop.id).first()
        draft_json = draft_record.draft_json if draft_record else None
    
    return render_template("program_billing/setup_wizard.html", 
        property=property,
        draft_json=json.dumps(draft_json) if draft_json else 'null')

@billing_bp.route("/billing/setup/submit", methods=["POST"])
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
            enrollment_id=_get_billing_enrollment_id(current_user.id),
            metro_arrangement_amount=float(data.get("metro_arrangement_amount") or 0.0),
            metro_arrangement_duration=int(data.get("metro_arrangement_duration") or 0),
            metro_rates_amount=float(data.get("metro_rates_amount") or 0.0)
        )
        db.session.add(prop)
        db.session.flush()
        
        # 2. Create Default Unit (Fix duplicate key crash)
        unit = BilProperty(
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
                email_statements=bool(data.get("email_statements")),
                property_id=prop.id
            )
            db.session.add(tenant)
            db.session.flush()
            
            rent_amount = data.get("rent_amount")
            lease = BilLease(
                tenant_id=tenant.id,
                property_id=prop.id,
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
                    property_id=prop.id,
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
                    property_id=prop.id,
                    parent_meter_id=parent_id,
                    pointing_to=m_data.get("pointing_to"),
                    municipal_bill_number=m_data.get("municipal_bill_number")
                )
                db.session.add(meter)
        
        db.session.commit()
        
        # 6. Sync Municipality Accounts and capture AI metrics
        from app.program_billing.helpers import sync_muni_accounts
        from app.models.billing import BilMuniAccount, BilMuniCycleTotals, RefMuniOwner
        from datetime import datetime
        
        sync_muni_accounts()
        
        muni_email = data.get("muni_email")
        initial_due = data.get("initial_due_amount")
        bill_no = data.get("metro_account_no")
        
        if not bill_no:
            for m in data.get("meters", []):
                if m.get("municipal_bill_number"):
                    bill_no = m.get("municipal_bill_number")
                    break
                    
        if bill_no:
            acc = BilMuniAccount.query.filter_by(account_number=bill_no).first()
            if not acc:
                owner_name = prop.manager.name if prop.manager else "Unknown Owner"
                owner = RefMuniOwner.query.filter_by(name=owner_name).first()
                if not owner:
                    owner = RefMuniOwner(name=owner_name)
                    db.session.add(owner)
                    db.session.flush()
                acc = BilMuniAccount(account_number=bill_no, owner_id=owner.id)
                db.session.add(acc)
                db.session.flush()
                
            if muni_email:
                acc.muni_email = muni_email
            if initial_due:
                month_str = datetime.utcnow().strftime("%Y-%m")
                cycle = BilMuniCycleTotals.query.filter_by(account_id=acc.id, period=month_str).first()
                if not cycle:
                    cycle = BilMuniCycleTotals(
                        account_id=acc.id,
                        period=month_str,
                        balance=float(initial_due),
                        due=float(initial_due)
                    )
                    db.session.add(cycle)
            db.session.commit()
                    
        flash("Property successfully set up!", "success")
        return redirect(url_for("billing_bp.learner_dashboard"))
        
    except Exception as e:
        db.session.rollback()
        import traceback
        traceback.print_exc()
        flash(f"An error occurred during setup: {str(e)}", "danger")
        return redirect(url_for("billing_bp.setup_wizard"))


@billing_bp.route("/billing/utilities/<int:property_id>/metsoa/<month>/email", methods=["POST"])
@login_required
def email_metsoa(property_id, month):
    from app.models.billing import BilProperty
    from flask import request
    from app.utils.mailer import send_pdf_email
    import tempfile
    import os
    
    data_req = request.get_json()
    email = data_req.get("email") if data_req else None
    if not email:
        return {"success": False, "error": "Email address is required"}, 400
        
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        return {"success": False, "error": "Unauthorized"}, 403

    elec_rows, elec_total = build_electrical_rows(prop.id, month)
    water_meters, water_total = build_water_rows(prop.id, month)
    
    grand_total = round(elec_total + water_total, 2)
    
    data = {
        "tenant": None,
        "property": prop,
        "month": month,
        "electricity": {
            "rows": elec_rows,
            "subtotal": elec_total
        },
        "water": {
            "meters": water_meters,
            "total": water_total
        },
        "grand_total": grand_total
    }

    html_string = render_template("program_billing/metsoa.html", **data)
    
    # Hide the action buttons in the PDF
    html_string = html_string.replace('class="print:hidden', 'style="display:none;"')
    
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        pdf_path = tmp.name

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(html_string, wait_until="networkidle")
            page.pdf(
                path=pdf_path, 
                format="A4", 
                print_background=True,
                display_header_footer=True,
                header_template="<span></span>",
                footer_template="<div style='width: 100%; text-align: center; font-size: 10px; color: #6b7280; padding-bottom: 10px;'>Page <span class='pageNumber'></span> of <span class='totalPages'></span></div>",
                margin={"top": "30px", "bottom": "40px", "left": "10px", "right": "10px"}
            )
            browser.close()
            
        with open(pdf_path, 'rb') as f_pdf:
            pdf_bytes = f_pdf.read()
            
        os.remove(pdf_path)
            
        subject = f"METSOA Review - {prop.name} - {month}"
        body = f"Hello,\n\nPlease find the METSOA statement for {prop.name} for the billing month of {month} attached as a PDF.\n\nRegards,\nAIT Platform"
        
        success = send_pdf_email(email, subject, body, pdf_bytes, filename=f"{month}-MetSoa-{prop.name}.pdf")
        if success:
            return {"success": True}
        else:
            return {"success": False, "error": "Mailer returned false."}
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
        return {"success": False, "error": str(e)}

@billing_bp.route("/metsoa/<int:property_id>/<month>/pdf")
@login_required
def metsoa_pdf(property_id, month):
    from app.models.billing import BilProperty
    import tempfile
    
    prop = BilProperty.query.get_or_404(property_id)

    if prop:
        manager_id = prop.manager_id
        from app.models.billing import BilStatementPayment
        payment = BilStatementPayment.query.filter_by(manager_id=manager_id, month=month).first()
        if not payment or payment.amount_paid_cents <= 0:
            if current_user.id == manager_id:
                flash(f"Please unlock statements for {month} before generating PDFs.", "warning")
                return redirect(url_for('billing_bp.billing_checkout', month=month))
            elif current_user.has_role('admin'):
                pass
            else:
                flash("Your manager has not unlocked statements for this month yet.", "danger")
                return redirect(url_for("public_bp.welcome"))

    elec_rows, elec_total = build_electrical_rows(prop.id, month)
    water_meters, water_total = build_water_rows(prop.id, month)
    
    grand_total = round(elec_total + water_total, 2)
    
    data = {
        "tenant": None,
        "property": prop,
        "month": month,
        "electricity": {
            "rows": elec_rows,
            "subtotal": elec_total
        },
        "water": {
            "meters": water_meters,
            "total": water_total
        },
        "grand_total": grand_total
    }

    html_string = render_template("program_billing/metsoa.html", **data)
    
    # Hide PDF button in PDF itself
    html_string = html_string.replace('href="{{ url_for(\'billing_bp.metsoa_pdf\', property_id=prop.id, month=month) }}"', 'style="display:none;"')
    
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        pdf_path = tmp.name

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(html_string, wait_until="networkidle")
            page.pdf(
                path=pdf_path, 
                format="A4", 
                print_background=True,
                display_header_footer=True,
                header_template="<span></span>",
                footer_template="<div style='width: 100%; text-align: center; font-size: 10px; color: #6b7280; padding-bottom: 10px;'>Page <span class='pageNumber'></span> of <span class='totalPages'></span></div>",
                margin={"top": "30px", "bottom": "40px", "left": "10px", "right": "10px"}
            )
            browser.close()
            
        return send_file(pdf_path, as_attachment=True, download_name=f"{month}-MetSoa-{prop.name}.pdf")
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash("Failed to generate PDF.", "danger")
        return redirect(url_for('billing_bp.metsoa', property_id=prop.id, month=month))

@billing_bp.route("/api/parse_readings", methods=["POST"])
@login_required
def parse_readings_api():
    if 'bill_file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
        
    file = request.files['bill_file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
        
    try:
        from google import genai
        from google.genai import types
        file_bytes = file.read()
        
        mime_type = file.mimetype
        if mime_type == 'application/pdf':
            mime_type = 'application/pdf'
        elif mime_type in ['image/jpeg', 'image/png']:
            pass
        else:
            return jsonify({"error": "Unsupported file type. Please upload a PDF, JPG, or PNG."}), 400
            
        
        
        prompt = '''
        Analyze this municipality bill and extract the specific meter readings for every meter found on the bill.
        Return the result strictly as a valid JSON object with a key "readings" containing an array of objects.
        Each object in the "readings" array should have:
        - "meter_number": The meter number (string)
        - "current_reading": The current reading value (number or string)
        - "current_date": The date of the current reading in YYYY-MM-DD format (string). If year is not given but month and day are, infer the most likely year based on the bill date.
        - "previous_reading": The previous reading value if listed (number or string)
        - "previous_date": The date of the previous reading in YYYY-MM-DD format (string)
        - "usage": The total consumption/usage amount for this meter during the billing period (number or string)
        
        If a previous_reading is missing but you have the current_reading and the usage, you MUST mathematically calculate the previous_reading (current_reading - usage = previous_reading) and include it.
        If a field cannot be determined and cannot be calculated, return an empty string for that field.
        Do not include markdown formatting like ```json.
        '''
        
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[types.Part.from_bytes(data=file_bytes, mime_type=mime_type), prompt]
        )
        
        text_response = response.text.strip()
        if text_response.startswith('```json'):
            text_response = text_response[7:]
        if text_response.endswith('```'):
            text_response = text_response[:-3]
            
        import json
        data = json.loads(text_response.strip())
        

        
        return jsonify(data)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Failed to parse bill: {str(e)}"}), 500

@billing_bp.route("/api/parse_bill_onboarding", methods=["POST"])
@login_required
def parse_bill_onboarding_api():
    if 'bill_files' not in request.files and 'bill_file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
        
    files = request.files.getlist('bill_files')
    if not files:
        files = request.files.getlist('bill_file')
        
    if not files or files[0].filename == '':
        return jsonify({"error": "No selected file(s)"}), 400
        
    try:
        from google import genai
        from google.genai import types
        import os
        import json
        from dotenv import load_dotenv
        from flask import current_app
        
        dotenv_path = os.path.join(current_app.root_path, '..', '.env')
        load_dotenv(dotenv_path, override=True)
        
        api_key = os.environ.get("GEMINI_API_KEY") or current_app.config.get("GEMINI_API_KEY")
        
        # Bulletproof fallback: manually parse .env if still missing
        if not api_key:
            try:
                with open(dotenv_path, 'r', encoding='utf-8') as ef:
                    for line in ef:
                        if line.startswith('GEMINI_API_KEY='):
                            api_key = line.split('=', 1)[1].strip().strip('"').strip("'")
                            break
            except Exception:
                pass

        if not api_key:
            return jsonify({"error": "GEMINI_API_KEY is not configured"}), 500
            
        client = genai.Client(api_key=api_key)
        
        prompt_parts = []
        for file in files:
            file_bytes = file.read()
            mime_type = file.mimetype
            if mime_type == 'application/pdf':
                pass
            elif mime_type in ['image/jpeg', 'image/png']:
                pass
            else:
                continue
            prompt_parts.append(types.Part.from_bytes(data=file_bytes, mime_type=mime_type))
            
        if not prompt_parts:
            return jsonify({"error": "Unsupported file type. Please upload a PDF, JPG, or PNG."}), 400
            
        
        
        prompt = f'''
        Analyze the provided municipality bill(s). There may be multiple files/images belonging to the same property.
        Extract BOTH the property details (finding the most common or 'Master' details) and the specific meter readings across ALL bills.
        This is typically an Ethekwini (Durban, KZN) municipality bill. 
        You MUST extract EVERY SINGLE meter reading found across ALL the bills. Do not summarize or skip any meters.
        
        CRITICAL INSTRUCTION: The user has specified that there are {request.form.get("sub_meters", "several")} sub-meters linked to a bulk meter. 
        Please ensure you extract ALL meters perfectly.
        ''' + '''
        Return the result strictly as a valid JSON object with the following structure:
        {
          "property_name": "Name of property or owner",
          "address": "Full address",
          "metro_account_no": "Municipal account number (usually 11 digits)",
          "muni_email": "Email address for the municipality (if present)",
          "has_rates": true,
          "rates_amount": 123.45,
          "amount_due": 1234.56,
          "readings": [
            {
              "meter_number": "meter number string",
              "utility_type": "water or electricity",
              "current_reading": 1234,
              "current_date": "YYYY-MM-DD",
              "previous_reading": 1200,
              "previous_date": "YYYY-MM-DD",
              "usage": 34
            }
          ]
        }
        
        Rules:
        - Ethekwini Water meters often end with letters like 'W' or 'S' (e.g. 123456W).
        - Ethekwini Electricity meters often end with letters like 'E', 'S', or 'X'.
        - Scan the "Meter Readings", "Electricity", and "Water" sections carefully.
        - If previous_reading is missing but you have current_reading and usage, calculate previous_reading (current - usage).
        - If year is missing in date, infer it based on bill date.
        - Infer utility_type from context (e.g. kWh means electricity, kL means water).
        - If a field is not found, return empty string or null.
        - Do not include markdown formatting like ```json.
        '''
        
        prompt_parts.append(prompt)
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt_parts)
        
        text_response = response.text.strip()
        if text_response.startswith('```json'):
            text_response = text_response[7:]
        if text_response.endswith('```'):
            text_response = text_response[:-3]
            
        data = json.loads(text_response.strip())
        
        try:
            from app.models.billing import BilExtractionLog
            from flask_login import current_user
            
            if current_user.is_authenticated:
                def _safe_float(val):
                    try:
                        return float(val) if val is not None else 0.0
                    except (ValueError, TypeError):
                        return 0.0
                        
                log_entry = BilExtractionLog(
                    manager_id=current_user.id,
                    property_name=data.get("property_name"),
                    address=data.get("address"),
                    metro_account_no=data.get("metro_account_no"),
                    muni_email=data.get("muni_email"),
                    has_rates=bool(data.get("has_rates")),
                    rates_amount=_safe_float(data.get("rates_amount")),
                    amount_due=_safe_float(data.get("amount_due")),
                    raw_json=data
                )
                from app.extensions import db
                db.session.add(log_entry)
                
                # Check if we should upgrade the draft property to collation
                prop_id = request.form.get("property_id")
                if prop_id:
                    draft = BilProperty.query.get(prop_id)
                    if draft and draft.onboarding_status == 'draft_extracting':
                        count = BilExtractionLog.query.filter_by(property_name=draft.name).count()
                        # We count the current one too because it's in the session but maybe not returned by count yet?
                        # Actually we can just do count + 1
                        if count + 1 >= draft.expected_bills:
                            draft.onboarding_status = 'draft_collating'
                            
                db.session.commit()
        except Exception as inner_e:
            import logging
            logging.error(f"Failed to save BilExtractionLog: {inner_e}")
            
        return jsonify(data)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Failed to parse bill: {str(e)}"}), 500

@billing_bp.route("/billing/onboarding", methods=["GET"])
@login_required
def ai_onboarding():
    property_id = request.args.get('property_id')
    draft_property = None
    if property_id:
        draft_property = BilProperty.query.get(property_id)
        if draft_property and draft_property.manager_id != current_user.id:
            from flask import abort
            abort(403)
    return render_template("program_billing/ai_onboarding.html", draft_property=draft_property)

@billing_bp.route("/billing/onboarding/process", methods=["POST"])
@login_required
def ai_onboarding_process():
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
        owner_unit = BilProperty(property_id=prop.id, name=f"{prop.name} - Owner/Common")
        db.session.add(owner_unit)
        db.session.flush()
        unit_map["owner"] = owner_property.id
        
        owner_tenant = BilTenant(name="Owner Account", property_id=owner_property.id)
        db.session.add(owner_tenant)
        db.session.flush()
        tenant_map["owner"] = owner_tenant.id
        db.session.add(BilLease(tenant_id=owner_tenant.id, property_id=owner_property.id, rent_amount=0))
        
        # 2b. Dynamic Tenants
        for t_data in data.get("tenants", []):
            tid = t_data.get("id")
            if not tid: continue
            tname = t_data.get("name", "").strip() or f"Statement {tid}"
            rent = float(t_data.get("rent") or 0.0)
            
            t_unit = BilProperty(property_id=prop.id, name=f"Unit {tid.upper()}")
            db.session.add(t_unit)
            db.session.flush()
            unit_map[tid] = t_property.id
            
            t_tenant = BilTenant(name=tname, property_id=t_property.id)
            db.session.add(t_tenant)
            db.session.flush()
            tenant_map[tid] = t_tenant.id
            db.session.add(BilLease(tenant_id=t_tenant.id, property_id=t_property.id, rent_amount=rent))
            
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
                u_id = unit_map.get(assign_to, owner_property.id)
                u_type = (m_data.get("utility_type") or "water").lower()
                
                meter = BilMeter(
                    meter_number=m_data.get("meter_number"),
                    utility_type=u_type,
                    property_id=u_id,
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
                u_id = unit_map.get(assign_to, owner_property.id)
                u_type = (m_data.get("utility_type") or "water").lower()
                
                parent_id = None
                if m_data.get("linked_to_bulk"):
                    parent_id = bulk_meters.get(u_type)
                
                meter = BilMeter(
                    meter_number=m_data.get("meter_number"),
                    utility_type=u_type,
                    property_id=u_id,
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

@billing_bp.route("/billing/onboarding/start_setup", methods=["POST"])
@login_required
def onboarding_start_setup():
    from app.extensions import db
    prop_name = request.form.get("property_name", "Draft Property").strip().title()

    # Check for duplicate property name (case-insensitive)
    existing_name = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        db.func.lower(BilProperty.name) == prop_name.lower()
    ).first()

    if existing_name:
        flash(f"You already have a property named '{prop_name}'. Please choose a different name.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))

    bills = 1
    tenants = 1
    is_bulk = "no"
    sub_meters = 0
    
    prop = BilProperty(
        name=prop_name,
        manager_id=current_user.id,
        enrollment_id=_get_billing_enrollment_id(current_user.id),
        onboarding_status='draft_extracting',
        expected_bills=bills,
        expected_tenants=tenants,
        is_bulk_metered=(1 if is_bulk == 'yes' else 0),
        expected_sub_meters=sub_meters
    )
    
    from app.extensions import db
    db.session.add(prop)
    db.session.commit()
    
    flash(f"Setup initialized for '{prop_name}'. You can now proceed with the Setup Wizard.", "success")
    return redirect(url_for('billing_bp.setup_wizard', property_id=prop.id))



# === RESTORED WIZARD ROUTES ===

@billing_bp.route('/save_architecture_draft/<int:property_id>', methods=['POST'])
@login_required
def save_architecture_draft(property_id):
    from app.models.billing import BilArchitectureDraft
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    draft = BilArchitectureDraft.query.filter_by(property_id=prop.id).first()
    if not draft:
        draft = BilArchitectureDraft(property_id=prop.id)
        from app.extensions import db
        db.session.add(draft)
    
    draft.draft_json = data
    from app.extensions import db
    db.session.commit()
    
    return jsonify({"status": "success"})

@billing_bp.route("/billing/onboarding/save_global_architecture/<int:property_id>", methods=["POST"])
@login_required
def save_global_architecture(property_id):
    try:
        from app.models.billing import BilArchitectureDraft
        prop = BilProperty.query.get_or_404(property_id)
        if prop.manager_id != current_user.id:
            return jsonify({"error": "Unauthorized"}), 403

        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        from app.extensions import db
        from app.models.billing import BilMuniAccount, RefMuniOwner, BilMeter
        from datetime import datetime
        
        # 1. Clean up old architecture
        old_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
        old_acc_nums = [a.account_number for a in old_accounts if a.account_number]
        
        if old_acc_nums:
            old_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(old_acc_nums)).all()
            old_meter_ids = [m.id for m in old_meters]
            if old_meter_ids:
                from app.models.billing import BilMeterReading, BilConsumption
                BilMeterReading.query.filter(BilMeterReading.meter_id.in_(old_meter_ids)).delete(synchronize_session=False)
                BilConsumption.query.filter(BilConsumption.meter_id.in_(old_meter_ids)).delete(synchronize_session=False)
                BilMeter.query.filter(BilMeter.id.in_(old_meter_ids)).delete(synchronize_session=False)
                
        BilMuniAccount.query.filter_by(property_id=prop.id).delete(synchronize_session=False)

        # 2. Process Owners
        owner_map = {} # acc.id (e.g. 'acc_0') -> owner_obj.id
        owner_data_map = {} # acc.id -> {'email': ..., 'address': ...}
        for o_data in data.get('owners', []):
            name = o_data.get('name', '').strip().title()
            acc_id = o_data.get('account_id')
            email = o_data.get('email', '').strip()
            address = o_data.get('address', '').strip()
            if name and acc_id:
                owner = RefMuniOwner.query.filter_by(name=name).first()
                if not owner:
                    owner = RefMuniOwner(name=name)
                    db.session.add(owner)
                    db.session.flush()
                owner_map[acc_id] = owner.id
                owner_data_map[acc_id] = {'email': email, 'address': address}

        # 3. Process Accounts & attach rates/arrears/arrangements
        acc_obj_map = {} # acc.id -> BilMuniAccount
        for a_data in data.get('accounts', []):
            acc_num = a_data.get('number', '').strip()
            acc_id = a_data.get('id')
            if acc_num and acc_id:
                acc = BilMuniAccount(
                    property_id=prop.id,
                    account_number=acc_num,
                    is_bulk_account=True if a_data.get('isBulk') else False
                )
                if acc_id in owner_map:
                    acc.owner_id = owner_map[acc_id]
                if acc_id in owner_data_map:
                    acc.muni_email = owner_data_map[acc_id].get('email')
                    acc.owner_address = owner_data_map[acc_id].get('address')
                db.session.add(acc)
                acc_obj_map[acc_id] = acc
        
        db.session.flush()

        # Attach Rates
        for r in data.get('rates', []):
            acc = acc_obj_map.get(r.get('account_id'))
            if acc:
                acc.rates_amount = float(r.get('amount') or 0.0)
                if r.get('date'):
                    acc.rates_date = datetime.strptime(r.get('date'), '%Y-%m-%d').date()
                acc.rates_charge_to = r.get('charge_to', 'owner')
                acc.rates_reference = r.get('reference', '')
                acc.rates_erf_details = r.get('erf_details', '')
                acc.rates_property_category = r.get('property_category', '')
                acc.rates_market_value = float(r.get('market_value') or 0.0)
                acc.rates_rateable_value = float(r.get('rateable_value') or 0.0)
                acc.rates_general_randage = float(r.get('general_randage') or 0.0)
                acc.rates_sra_randage = float(r.get('sra_randage') or 0.0)
                acc.rates_deferred = float(r.get('deferred') or 0.0)
                acc.rates_sra_monthly = float(r.get('sra_monthly') or 0.0)
                acc.rates_general_monthly = float(r.get('general_monthly') or 0.0)

        # Attach Arrears
        for a in data.get('arrears', []):
            acc = acc_obj_map.get(a.get('account_id'))
            if acc:
                acc.arrears_amount = float(a.get('amount') or 0.0)
                if a.get('date'):
                    acc.arrears_date = datetime.strptime(a.get('date'), '%Y-%m-%d').date()
                acc.arrears_charge_to = a.get('charge_to', 'owner')
                
        # Attach Arrangements
        for arg in data.get('arrangements', []):
            acc = acc_obj_map.get(arg.get('account_id'))
            if acc:
                acc.arrangement_contract_number = arg.get('contract_number', '')
                acc.arrangement_charge_to = arg.get('charge_to', 'owner')
                acc.arrangement_agreement_amount = float(arg.get('agreement_amount') or 0.0)
                acc.arrangement_installments_raised = float(arg.get('installments_raised') or 0.0)
                acc.arrangement_installment_amount = float(arg.get('installment_amount') or 0.0)
                acc.arrangement_amount_owing = float(arg.get('amount_owing') or 0.0)
                acc.arrangement_remaining_periods = int(arg.get('remaining_periods') or 0)
                if arg.get('date'):
                    acc.arrangement_date = datetime.strptime(arg.get('date'), '%Y-%m-%d').date()

        # 4. Process Meters
        # Extract meters and map them properly from frontend payload
        raw_meters = []
        for u_type, key in [('Water', 'bulkWater'), ('Electrical', 'bulkElec'), ('Water', 'subWater'), ('Electrical', 'subElec')]:
            for m_item in data.get(key, []):
                m_item['u_type'] = u_type
                m_item['is_bulk'] = key.startswith('bulk')
                raw_meters.append(m_item)
                
        # Build account map
        acc_map = { a.get('id'): a.get('number', '').strip() for a in data.get('accounts', []) }
        
        # Find the bulk account
        bulk_acc_num = ''
        for a in data.get('accounts', []):
            if a.get('isBulk'):
                bulk_acc_num = a.get('number', '').strip()
                break
        if not bulk_acc_num and data.get('accounts'):
            bulk_acc_num = data.get('accounts')[0].get('number', '').strip()
        
        # Build meter-to-account map { meter_id: account_number }
        meter_acc = {}
        for mp in data.get('mapping', []):
            acc_id = mp.get('account_id')
        for m_item in data.get('bulkWater', []):
            meter_acc[m_item.get('id')] = bulk_acc_num
        for m_item in data.get('bulkElec', []):
            meter_acc[m_item.get('id')] = bulk_acc_num
            
        # For sub meters, read from mapping
        for m_map in data.get('mapping', []):
            acc_id = m_map.get('account_id')
            w_id = m_map.get('water')
            e_id = m_map.get('elec')
            if acc_id in acc_map:
                if w_id:
                    meter_acc[w_id] = acc_map[acc_id]
                if e_id:
                    meter_acc[e_id] = acc_map[acc_id]
                    
        # Fallback if mapping is empty (like when only 1 account)
        if len(acc_map) == 1:
            only_acc_num = list(acc_map.values())[0]
            for m_item in data.get('subWater', []):
                meter_acc[m_item.get('id')] = only_acc_num
            for m_item in data.get('subElec', []):
                meter_acc[m_item.get('id')] = only_acc_num
    
        active_meters_map = {}
                
        for m_data in raw_meters:
            m_num = m_data.get('number', '').strip()
            m_id = m_data.get('id')
            u_type = m_data.get('u_type')
            
            # Determine account number
            acc_num = bulk_acc_num if m_data.get('is_bulk') else meter_acc.get(m_id, '')
            
            if m_num and acc_num:
                meter = BilMeter(
                    meter_number=m_num,
                    utility_type=u_type,
                    municipal_bill_number=acc_num
                )
                db.session.add(meter)
                db.session.flush() # get meter.id
                active_meters_map[m_id] = meter
                
                initial_readings = data.get('initialReadings', [])
                for r_data in initial_readings:
                    if str(r_data.get('meter_number', '')) == str(m_num):
                        from datetime import datetime
                        from app.models.billing import BilMeterReading
                        rd_date = r_data.get('date')
                        rd_val = r_data.get('value')
                        if rd_date and rd_val is not None:
                            try:
                                parsed_date = datetime.strptime(rd_date, '%Y-%m-%d').date()
                                reading = BilMeterReading(
                                    meter_id=meter.id,
                                    reading_date=parsed_date,
                                    reading_value=float(rd_val)
                                )
                                db.session.add(reading)
                            except:
                                pass

        # Handle Exceptions (Stolen Meters)
        for exc in data.get('exceptions', []):
            s_num = exc.get('stolen_num', '').strip()
            r_id = exc.get('replacement_id', '')
            d_stolen = exc.get('date_stolen')
            d_replaced = exc.get('date_replaced')
            
            if s_num and r_id in active_meters_map:
                rep_meter = active_meters_map[r_id]
                
                from datetime import datetime
                try:
                    parsed_ds = datetime.strptime(d_stolen, '%Y-%m-%d').date() if d_stolen else None
                    parsed_dr = datetime.strptime(d_replaced, '%Y-%m-%d').date() if d_replaced else None
                except:
                    parsed_ds = None
                    parsed_dr = None
                    
                stolen_meter = BilMeter(
                    meter_number=s_num,
                    utility_type=rep_meter.utility_type,
                    municipal_bill_number=rep_meter.municipal_bill_number,
                    status='stolen',
                    date_stolen=parsed_ds,
                    date_replaced=parsed_dr
                )
                db.session.add(stolen_meter)
                db.session.flush()
                
                rep_meter.replacement_for_meter_id = stolen_meter.id

        prop.onboarding_status = 'draft_manual'
        
        # Save Draft JSON so frontend wizard can restore it!
        from app.models.billing import BilArchitectureDraft
        draft = BilArchitectureDraft.query.filter_by(property_id=prop.id).first()
        if not draft:
            draft = BilArchitectureDraft(property_id=prop.id)
            db.session.add(draft)
        draft.draft_json = data
        
        db.session.commit()
        return jsonify({"message": "Architecture saved successfully!"}), 200
        
    except Exception as e:
        from app.extensions import db
        import traceback
        try:
            db.session.rollback()
        except:
            pass
        print("SAVE GLOBAL ARCHITECTURE ERROR:", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@billing_bp.route('/property/<int:property_id>/email_architecture_summary', methods=['GET', 'POST'])
@login_required
def email_architecture_summary(property_id):
    flash("Email feature coming soon!", "info")
    return redirect(url_for('billing_bp.architecture_summary', property_id=property_id))

@billing_bp.route('/property/<int:property_id>/architecture_summary')
@login_required
def architecture_summary(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        flash("Unauthorized access.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    
    # Gather data for summary
    from app.models.billing import BilMuniAccount, BilMeter, RefMuniOwner
    accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    
    acc_nums = [a.account_number for a in accounts if a.account_number]
    if acc_nums:
        meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(acc_nums)).all()
    else:
        meters = []
        
    account_meters = {}
    for acc in accounts:
        if acc.account_number:
            account_meters[acc.account_number] = {
                'water': [m for m in meters if m.municipal_bill_number == acc.account_number and 'water' in (m.utility_type or '').lower()],
                'elec': [m for m in meters if m.municipal_bill_number == acc.account_number and 'elec' in (m.utility_type or '').lower()]
            }

    bulk_water = []
    bulk_elec = []
    sub_water = []
    sub_elec = []
    exceptions = [] # Exceptions not fully implemented in db yet
    
    owner_ids = list(set([a.owner_id for a in accounts if a.owner_id]))
    if owner_ids:
        owners = RefMuniOwner.query.filter(RefMuniOwner.id.in_(owner_ids)).all()
    else:
        owners = []
    
    # Extract unique owners
    owner_map = {}
    for acc in accounts:
        if acc.owner:
            owner_map[acc.owner.name] = {
                'name': acc.owner.name,
                'email_address': acc.muni_email or '-',
                'address': acc.owner_address or '-'
            }
    owners = list(owner_map.values())
    
    for m in meters:
        # Determine bulk vs sub
        acc = next((a for a in accounts if a.account_number == m.municipal_bill_number), None)
        is_bulk = acc.is_bulk_account if acc else False
        
        if 'water' in (m.utility_type or '').lower():
            if is_bulk:
                bulk_water.append(m)
            else:
                sub_water.append(m)
        else:
            if is_bulk:
                bulk_elec.append(m)
            else:
                sub_elec.append(m)

    return render_template('program_billing/architecture_summary.html', 
                           property=prop, account_meters=account_meters, 
                           accounts=accounts,
                           bulk_water=bulk_water,
                           bulk_elec=bulk_elec,
                           sub_water=sub_water,
                           sub_elec=sub_elec,
                           exceptions=exceptions,
                           owners=owners)

@billing_bp.route("/api/save_reading", methods=["POST"])
@login_required
def save_reading():
    from app.models import BilMeter, BilConsumption
    data = request.json
    meter_id = data.get("meter_id")
    reading_month = data.get("reading_month")
    
    if not meter_id or not reading_month:
        return {"success": False, "error": "Missing meter_id or reading_month"}, 400
        
    m = BilMeter.query.get(meter_id)
    if not m:
        return {"success": False, "error": "Meter not found"}, 404
        
    if data.get("estimated_consumption") is not None:
        est_cons = float(data.get("estimated_consumption"))
        date_str = data.get("date")
        from datetime import datetime
        new_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        BilConsumption.query.filter_by(meter_id=m.id, month=reading_month).delete()
        
        cons_obj = BilConsumption(
            meter_id=m.id,
            meter_number=m.meter_number,
            last_date=new_date,
            new_date=new_date,
            last_read=0,
            new_read=0,
            days=30,
            consumption=est_cons,
            month=reading_month
        )
        from app.extensions import db
        db.session.add(cons_obj)
        db.session.commit()
        return {"success": True, "message": "Saved"}
        
    # Standard reading save logic
    from datetime import datetime
    new_date = datetime.strptime(data.get("date"), "%Y-%m-%d").date()
    new_reading = float(data.get("reading"))
    
    BilConsumption.query.filter_by(meter_id=m.id, month=reading_month).delete()
    
    prev_reading = data.get("prev_reading")
    prev_date_str = data.get("prev_date")
    
    if prev_reading is not None and prev_reading != "":
        prev_reading = float(prev_reading)
        if prev_date_str:
            prev_date = datetime.strptime(prev_date_str, "%Y-%m-%d").date()
        else:
            from dateutil.relativedelta import relativedelta
            prev_date = new_date - relativedelta(days=30)
    else:
        # Query the database for the most recent reading before new_date
        from app.models.billing import BilMeterReading
        last_r = BilMeterReading.query.filter(
            BilMeterReading.meter_id == m.id,
            BilMeterReading.reading_date < new_date
        ).order_by(BilMeterReading.reading_date.desc()).first()
        
        if last_r:
            prev_reading = last_r.reading_value
            prev_date = last_r.reading_date
        else:
            prev_reading = 0
            from dateutil.relativedelta import relativedelta
            prev_date = new_date - relativedelta(days=30)
        
    days = (new_date - prev_date).days
    if days == 0: days = 30
    
    cons_val = new_reading - prev_reading
    if cons_val < 0: cons_val = 0
    
    cons_obj = BilConsumption(
        meter_id=m.id,
        meter_number=m.meter_number,
        last_date=prev_date,
        new_date=new_date,
        last_read=prev_reading,
        new_read=new_reading,
        days=days,
        consumption=cons_val,
        month=reading_month
    )
    
    from app.extensions import db
    db.session.add(cons_obj)
    db.session.commit()
    return {"success": True, "message": "Saved"}


@billing_bp.route("/meter_exceptions/<int:property_id>", methods=["GET"])
@login_required
def meter_exceptions(property_id):
    from app.models.billing import BilProperty, BilMeter
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        from flask import abort
        abort(403)
        
    all_meters = get_all_property_meters(property_id)
    # Filter for non-active meters (like stolen)
    exception_meters = [m for m in all_meters if (m.status or 'active').lower() != 'active']
    
    # We want to find the replacement meter for each exception meter
    # Replacement meter points to the stolen meter via replacement_for_meter_id
    replacement_map = {}
    for ex in exception_meters:
        rep = BilMeter.query.filter_by(replacement_for_meter_id=ex.id).first()
        replacement_map[ex.id] = rep
        
    return render_template("program_billing/meter_exceptions.html", 
                           property=prop, 
                           exception_meters=exception_meters,
                           replacement_map=replacement_map)


@billing_bp.route("/billing/utilities", methods=["GET", "POST"])
@login_required
def utilities_hub():
    from app.models.billing import BilProperty
    from datetime import datetime
    
    properties = BilProperty.query.filter_by(manager_id=current_user.id).all()
    current_month = datetime.now().strftime("%Y-%m")
    
    if request.method == "POST":
        property_id = request.form.get("property_id")
        month = request.form.get("billing_month")
        action = request.form.get("action")
        
        if not property_id or not month:
            flash("Please select both a property and a month.", "warning")
            return redirect(url_for("billing_bp.utilities_hub"))
            
        if action == "consumption":
            return redirect(url_for("billing_bp.consumption_review", property_id=property_id, month=month))
        elif action == "metsoa":
            return redirect(url_for("billing_bp.metsoa", property_id=property_id, month=month))
        elif action == "exceptions":
            return redirect(url_for("billing_bp.exception_metsoa", property_id=property_id, month=month))

    return render_template("program_billing/utilities_hub.html", properties=properties, current_month=current_month)

@billing_bp.route("/billing/utilities/<int:property_id>/consumption/<month>")
@login_required
def consumption_review(property_id, month):
    from app.models.billing import BilProperty, BilMuniAccount, BilMeter, BilConsumption
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    all_meters = BilMeter.query.filter_by(property_id=property_id).all()
        
    muni_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    muni_acc_numbers = [acc.account_number for acc in muni_accounts if acc.account_number]
    if muni_acc_numbers:
        muni_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(muni_acc_numbers)).all()
        for m in muni_meters:
            if m not in all_meters:
                all_meters.append(m)
                
    for acc in muni_accounts:
        if acc.water_meter and acc.water_meter not in all_meters:
            all_meters.append(acc.water_meter)
        if acc.elec_meter and acc.elec_meter not in all_meters:
            all_meters.append(acc.elec_meter)
            
    # Get consumption records for these meters for this month
    meter_ids = [m.id for m in all_meters]
    if meter_ids:
        consumptions = BilConsumption.query.filter(
            BilConsumption.meter_id.in_(meter_ids),
            BilConsumption.month == month
        ).all()
    else:
        consumptions = []
    
    cons_map = {c.meter_id: c for c in consumptions}
    
    data = []
    for m in all_meters:
        c = cons_map.get(m.id)
        data.append({
            'meter': m,
            'consumption': c
        })
        
    return render_template("program_billing/consumption_table.html", property=prop, month=month, data=data)


@billing_bp.route("/billing/soa", methods=["GET", "POST"])
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
    
    selected_account_id = request.args.get('account_id')
    selected_account = None
    if selected_account_id:
        selected_account = BilMuniAccount.query.filter_by(id=selected_account_id, property_id=prop.id).first()
        
    return render_template("program_billing/soa_map.html", property=prop, month=month, muni_accounts=muni_accounts, selected_account=selected_account)

@billing_bp.route("/billing/soa/tenants/<int:property_id>/<month>", methods=["GET"])
@login_required
def soa_tenants_view(property_id, month):
    from app.models.billing import BilProperty
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
    tenants = prop.tenants
    return render_template("program_billing/soa_tenants.html", property=prop, month=month, tenants=tenants)

@billing_bp.route("/billing/soa/tenants/<int:property_id>/<month>/add", methods=["POST"])
@login_required
def soa_add_tenant(property_id, month):
    from app.models.billing import BilProperty, BilTenant, BilLease
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        


    name = request.form.get("name")
    if not name:
        flash("Tenant name is required.", "danger")
        return redirect(url_for("billing_bp.soa_tenants_view", property_id=property_id, month=month))
        
    status = request.form.get("status") == "active"
    date_started_str = request.form.get("date_started")
    date_terminated_str = request.form.get("date_terminated")
    
    date_started = None
    if date_started_str:
        try:
            date_started = datetime.strptime(date_started_str, "%Y-%m-%d").date()
        except ValueError:
            pass

    date_terminated = None
    if date_terminated_str:
        try:
            date_terminated = datetime.strptime(date_terminated_str, "%Y-%m-%d").date()
        except ValueError:
            pass

    new_tenant = BilTenant(
        name=name,
        property_id=prop.id,
        is_active=status,
        date_started=date_started,
        date_terminated=date_terminated
    )
    db.session.add(new_tenant)
    db.session.flush()

    # Create a blank lease to ensure the config views work
    lease = BilLease(
        tenant_id=new_tenant.id,
        property_id=prop.id,
        start_date=date_started_str if date_started_str else None,
        end_date=date_terminated_str if date_terminated_str else None,
        rent_amount=0.0
    )
    db.session.add(lease)
    
    db.session.commit()
    flash(f"Tenant {name} added successfully.", "success")
    return redirect(url_for("billing_bp.soa_tenants_view", property_id=property_id, month=month))

@billing_bp.route("/billing/soa/generate/<int:property_id>/<month>", methods=["GET"])
@login_required
def soa_generate_view(property_id, month):
    from app.models.billing import BilProperty
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
    tenants = prop.tenants
    return render_template("program_billing/soa_generate.html", property=prop, month=month, tenants=tenants)


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
    from app.models.billing import BilProperty
    prop = BilProperty.query.get(acc.property_id) if acc.property_id else None
    if prop and prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    acc.arrears_charge_to = request.form.get("arrears_charge_to", "owner")
    acc.rates_charge_to = request.form.get("rates_charge_to", "owner")
    acc.arrangement_charge_to = request.form.get("arrangement_charge_to", "owner")
    
    # Save the input amounts
    try:
        acc.rates_amount = float(request.form.get("rates_amount") or 0.0)
    except:
        pass
    try:
        acc.arrears_amount = float(request.form.get("arrears_amount") or 0.0)
    except:
        pass
    try:
        acc.ca_installment_amount = float(request.form.get("ca_installment_amount") or 0.0)
    except:
        pass
        
    db.session.commit()
    flash("SOA Map recorded successfully.", "success")
    return redirect(url_for("billing_bp.soa_map_view", property_id=property_id, month=month, account_id=account_id))


@billing_bp.route("/billing/soa/tenant/<int:tenant_id>/edit", methods=["GET", "POST"])
@login_required
def edit_tenant_soa(tenant_id):
    from app.models.billing import BilTenant
    from app.extensions import db
    from datetime import datetime
    
    tenant = BilTenant.query.get_or_404(tenant_id)
    
    # Very basic validation that the user owns the property
    if tenant.property and tenant.property:
        if tenant.property.manager_id != current_user.id and not current_user.has_role('admin'):
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

@billing_bp.route("/billing/soa/tenant/<int:tenant_id>/<month>/email", methods=["POST"])
@login_required
def email_soa(tenant_id, month):
    from app.models.billing import BilTenant, BilMuniAccount
    import tempfile
    import os
    from flask import send_file, request
    from app.utils.mailer import send_pdf_email
    
    tenant = BilTenant.query.get_or_404(tenant_id)
    
    data_req = request.get_json()
    email = data_req.get("email") if data_req else None
    if not email:
        return {"success": False, "error": "Email address is required"}, 400
        
    prop = tenant.property
    
    tenant_meter_ids = [m.id for m in tenant.property.meters]
    
    elec_rows, elec_total = build_electrical_rows(prop.id, month, filter_meter_ids=tenant_meter_ids)
    water_meters, water_total = build_water_rows(prop.id, month, filter_meter_ids=tenant_meter_ids)
    
    # Calculate mapped charges
    muni_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    
    mapped_charges = []
    mapped_total = 0.0
    
    for acc in muni_accounts:
        if acc.rates_charge_to == 'tenant':
            val = acc.rates_amount if acc.rates_amount and acc.rates_amount > 0 else round((acc.rates_general_monthly or 0) + (acc.rates_sra_monthly or 0), 2)
            if val > 0:
                mapped_charges.append({"description": "Rates & SRA", "amount": val})
                mapped_total += val
                
        if acc.arrears_charge_to == 'tenant':
            if acc.arrears_amount and acc.arrears_amount > 0:
                mapped_charges.append({"description": "Arrears", "amount": acc.arrears_amount})
                mapped_total += acc.arrears_amount
                
        if acc.arrangement_charge_to == 'tenant':
            if acc.ca_installment_amount and acc.ca_installment_amount > 0:
                mapped_charges.append({"description": "Arrangement Installment", "amount": acc.ca_installment_amount})
                mapped_total += acc.ca_installment_amount
                
    grand_total = round(elec_total + water_total + mapped_total, 2)
    
    data = {
        "tenant": tenant,
        "property": prop,
        "month": month,
        "electricity": {
            "rows": elec_rows,
            "subtotal": elec_total
        },
        "water": {
            "meters": water_meters,
            "total": water_total
        },
        "mapped_charges": mapped_charges,
        "mapped_total": mapped_total,
        "grand_total": grand_total
    }
    
    html_string = render_template("program_billing/soa_document.html", **data)
    
    # Hide the action buttons in the PDF
    html_string = html_string.replace('class="print:hidden', 'style="display:none;"')
    
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        pdf_path = tmp.name

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(html_string, wait_until="networkidle")
            page.pdf(
                path=pdf_path, 
                format="A4", 
                print_background=True,
                display_header_footer=True,
                header_template="<span></span>",
                footer_template="<div style='width: 100%; text-align: center; font-size: 10px; color: #6b7280; padding-bottom: 10px;'>Page <span class='pageNumber'></span> of <span class='totalPages'></span></div>",
                margin={"top": "30px", "bottom": "40px", "left": "20px", "right": "20px"}
            )
            browser.close()
            
        with open(pdf_path, 'rb') as f_pdf:
            pdf_bytes = f_pdf.read()
            
        os.remove(pdf_path)
            
        subject = f"Statement of Account - {tenant.name} - {month}"
        body = f"Hello {tenant.name},\n\nPlease find your Statement of Account for the billing month of {month} attached as a PDF.\n\nRegards,\n{prop.name} Management"
        
        success = send_pdf_email(email, subject, body, pdf_bytes, filename=f"SOA_{tenant.name.replace(' ', '_')}_{month}.pdf")
        if success:
            return {"success": True}
        else:
            return {"success": False, "error": "Mailer returned false."}
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
        return {"success": False, "error": str(e)}

@billing_bp.route("/billing/soa/tenant/<int:tenant_id>/generate", methods=["GET"])
@login_required
def generate_soa(tenant_id):
    from app.models.billing import BilTenant, BilMuniAccount
    import tempfile
    import os
    from flask import send_file
    
    tenant = BilTenant.query.get_or_404(tenant_id)
    month = request.args.get("month")
    
    if not month:
        flash("Month is required.", "danger")
        return redirect(url_for("billing_bp.soa_dashboard"))
        
    prop = tenant.property
    
    tenant_meter_ids = [m.id for m in tenant.property.meters]
    
    elec_rows, elec_total = build_electrical_rows(prop.id, month, filter_meter_ids=tenant_meter_ids)
    water_meters, water_total = build_water_rows(prop.id, month, filter_meter_ids=tenant_meter_ids)
    
    # Calculate mapped charges
    muni_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    
    mapped_charges = []
    mapped_total = 0.0
    
    for acc in muni_accounts:
        if acc.rates_charge_to == 'tenant':
            val = acc.rates_amount if acc.rates_amount and acc.rates_amount > 0 else round((acc.rates_general_monthly or 0) + (acc.rates_sra_monthly or 0), 2)
            if val > 0:
                mapped_charges.append({"description": "Rates & SRA", "amount": val})
                mapped_total += val
                
        if acc.arrears_charge_to == 'tenant':
            if acc.arrears_amount and acc.arrears_amount > 0:
                mapped_charges.append({"description": "Arrears", "amount": acc.arrears_amount})
                mapped_total += acc.arrears_amount
                
        if acc.arrangement_charge_to == 'tenant':
            if acc.ca_installment_amount and acc.ca_installment_amount > 0:
                mapped_charges.append({"description": "Arrangement Installment", "amount": acc.ca_installment_amount})
                mapped_total += acc.ca_installment_amount
                
    grand_total = round(elec_total + water_total + mapped_total, 2)
    
    data = {
        "tenant": tenant,
        "property": prop,
        "month": month,
        "electricity": {
            "rows": elec_rows,
            "subtotal": elec_total
        },
        "water": {
            "meters": water_meters,
            "total": water_total
        },
        "mapped_charges": mapped_charges,
        "mapped_total": mapped_total,
        "grand_total": grand_total
    }
    
    html_string = render_template("program_billing/soa_document.html", **data)
    
    # If the user clicks print in the browser
    if request.args.get("view") == "html":
        return html_string
        
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        pdf_path = tmp.name

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(html_string, wait_until="networkidle")
            page.pdf(
                path=pdf_path, 
                format="A4", 
                print_background=True,
                display_header_footer=True,
                header_template="<span></span>",
                footer_template="<div style='width: 100%; text-align: center; font-size: 10px; color: #6b7280; padding-bottom: 10px;'>Page <span class='pageNumber'></span> of <span class='totalPages'></span></div>",
                margin={"top": "30px", "bottom": "40px", "left": "20px", "right": "20px"}
            )
            browser.close()
            
        return send_file(pdf_path, as_attachment=True, download_name=f"SOA_{tenant.name.replace(' ', '_')}_{month}.pdf")
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f"Failed to generate SOA: {str(e)}", "danger")
        return redirect(url_for('billing_bp.soa_dashboard', property_id=prop.id, month=month))



@billing_bp.route("/billing/utilities/<int:property_id>/consumption/<month>/email", methods=["POST"])
@login_required
def email_consumption(property_id, month):
    from app.models.billing import BilProperty, BilMuniAccount, BilMeter, BilConsumption
    from app.utils.mailer import send_pdf_email
    from app.utils.pdf_render import html_to_pdf_bytes
    from flask import request, current_app, render_template
    
    data = request.get_json()
    email = data.get("email") if data else None
    
    if not email:
        return {"success": False, "error": "Email address is required"}, 400
        
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        return {"success": False, "error": "Unauthorized"}, 403
        
    all_meters = BilMeter.query.filter_by(property_id=property_id).all()
        
    muni_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    muni_acc_numbers = [acc.account_number for acc in muni_accounts if acc.account_number]
    if muni_acc_numbers:
        muni_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(muni_acc_numbers)).all()
        for m in muni_meters:
            if m not in all_meters:
                all_meters.append(m)
                
    for acc in muni_accounts:
        if acc.water_meter and acc.water_meter not in all_meters:
            all_meters.append(acc.water_meter)
        if acc.elec_meter and acc.elec_meter not in all_meters:
            all_meters.append(acc.elec_meter)
            
    meter_ids = [m.id for m in all_meters]
    if meter_ids:
        consumptions = BilConsumption.query.filter(
            BilConsumption.meter_id.in_(meter_ids),
            BilConsumption.month == month
        ).all()
    else:
        consumptions = []
    
    cons_map = {c.meter_id: c for c in consumptions}
    
    data = []
    for m in all_meters:
        c = cons_map.get(m.id)
        data.append({
            'meter': m,
            'consumption': c
        })
        
    html = render_template("program_billing/consumption_table_pdf.html", property=prop, month=month, data=data)
    
    try:
        pdf_bytes = html_to_pdf_bytes(html, orientation="Landscape")
    except Exception as e:
        return {"success": False, "error": "Failed to generate PDF: " + str(e)}
        
    subject = f"Consumption Review - {prop.name} - {month}"
    body = f"Hello,\n\nPlease find the consumption review for {prop.name} for the billing month of {month} attached as a PDF.\n\nRegards,\nAIT Platform"
    
    try:
        success = send_pdf_email(email, subject, body, pdf_bytes, filename=f"Consumption_Review_{prop.name}_{month}.pdf")
        if success:
            return {"success": True}
        else:
            return {"success": False, "error": "Mailer returned false."}
    except Exception as e:
        return {"success": False, "error": str(e)}
@billing_bp.route("/billing/checkout/<month>/unlock", methods=["POST"])
@login_required
def billing_unlock(month):
    from app.models.billing import BilStatementPayment, BilProperty
    
    # Check if already paid
    payment = BilStatementPayment.query.filter_by(manager_id=current_user.id, month=month).first()
    if payment and payment.amount_paid_cents > 0:
        flash("You have already unlocked statements for this month.", "info")
        return redirect(url_for('billing_bp.utilities_hub'))
        
    cost_cents = session.get("metro_billing_amount_cents", 0)
    meters_billed = session.get("metro_billing_meters", 0)
    
    # Check trial
    is_trial = True # Hardcoded for testing
    
    if not is_trial:
        main_prop = BilProperty.query.filter_by(manager_id=current_user.id).first()
        if not main_prop or main_prop.wallet_balance_cents < cost_cents:
            flash("Insufficient tokens to unlock.", "danger")
            return redirect(url_for('billing_bp.billing_checkout', month=month))
        # Deduct
        main_prop.wallet_balance_cents -= cost_cents
        db.session.add(main_prop)
    
    new_payment = BilStatementPayment(
        manager_id=current_user.id,
        month=month,
        meters_billed=meters_billed,
        amount_paid_cents=cost_cents if not is_trial else 1 # Just needs to be > 0
    )
    db.session.add(new_payment)
    db.session.commit()
    
    flash(f"Successfully unlocked statements for {month}!", "success")
    return redirect(url_for('billing_bp.utilities_hub'))
