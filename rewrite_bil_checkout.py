from app import create_app
from app.extensions import db
from sqlalchemy import text
import re

app = create_app()

with app.app_context():
    with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
        content = f.read()

    new_checkout = """
@billing_bp.route('/billing/checkout/<month>')
@login_required
def billing_checkout(month):
    from app.models.billing import BilStatementPayment, BilProperty, BilSectionalUnit, BilMeter
    from sqlalchemy import text
    from datetime import datetime
    
    payment = BilStatementPayment.query.filter_by(manager_id=current_user.id, month=month).first()
    if payment and payment.amount_paid_cents > 0:
        flash("You have already unlocked statements for this month.", "info")
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    props = BilProperty.query.filter_by(manager_id=current_user.id).all()
    if not props:
        flash("You have no properties to bill.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    prop_ids = [p.id for p in props]
    units = BilSectionalUnit.query.filter(BilSectionalUnit.property_id.in_(prop_ids)).all()
    unit_ids = [u.id for u in units]
    meters = BilMeter.query.filter(BilMeter.sectional_unit_id.in_(unit_ids)).all()
    meter_count = len(meters)
    
    setting = db.session.execute(text("SELECT value FROM system_settings WHERE key = 'billing_statement_cents'")).fetchone()
    statement_cost = int(setting[0]) if setting else 500
    total_cost_cents = meter_count * statement_cost
    
    main_prop = props[0]
    is_trial = main_prop.trial_ends_at and datetime.utcnow() < main_prop.trial_ends_at
    
    return render_template("program_billing/checkout_summary.html", 
                           month=month, 
                           meter_count=meter_count, 
                           statement_cost=statement_cost,
                           total_cost_cents=total_cost_cents,
                           main_prop=main_prop,
                           is_trial=is_trial)

@billing_bp.route('/billing/unlock/<month>', methods=["POST"])
@login_required
def billing_unlock(month):
    from app.models.billing import BilStatementPayment, BilProperty, BilSectionalUnit, BilMeter
    from sqlalchemy import text
    from datetime import datetime
    
    payment = BilStatementPayment.query.filter_by(manager_id=current_user.id, month=month).first()
    if payment and payment.amount_paid_cents > 0:
        flash("Already unlocked.", "info")
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    props = BilProperty.query.filter_by(manager_id=current_user.id).all()
    if not props:
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    prop_ids = [p.id for p in props]
    units = BilSectionalUnit.query.filter(BilSectionalUnit.property_id.in_(prop_ids)).all()
    unit_ids = [u.id for u in units]
    meters = BilMeter.query.filter(BilMeter.sectional_unit_id.in_(unit_ids)).all()
    meter_count = len(meters)
    
    setting = db.session.execute(text("SELECT value FROM system_settings WHERE key = 'billing_statement_cents'")).fetchone()
    statement_cost = int(setting[0]) if setting else 500
    total_cost_cents = meter_count * statement_cost
    
    main_prop = props[0]
    
    if main_prop.trial_ends_at and datetime.utcnow() < main_prop.trial_ends_at:
        main_prop.shadow_spent_cents += total_cost_cents
    else:
        if main_prop.wallet_balance_cents < total_cost_cents:
            flash("Insufficient tokens. Please top up or pay your registration fee.", "warning")
            return redirect(url_for('billing_bp.mock_bill'))
        main_prop.wallet_balance_cents -= total_cost_cents
        
    new_payment = BilStatementPayment(manager_id=current_user.id, month=month, amount_paid_cents=total_cost_cents)
    db.session.add(new_payment)
    db.session.commit()
    
    flash(f"Unlocked statements for {month} successfully!", "success")
    return redirect(url_for('billing_bp.learner_dashboard'))
    
@billing_bp.route('/billing/mock_bill')
@login_required
def mock_bill():
    from app.models.billing import BilProperty
    main_prop = BilProperty.query.filter_by(manager_id=current_user.id).first()
    if not main_prop:
        return redirect(url_for('public_bp.welcome'))
    return render_template('program_billing/mock_bill.html', prop=main_prop)
    
@billing_bp.route('/billing/topup')
@login_required
def topup():
    from app.models.billing import BilProperty
    main_prop = BilProperty.query.filter_by(manager_id=current_user.id).first()
    if not main_prop:
        return redirect(url_for('public_bp.welcome'))
    # Dummy topup for now
    main_prop.wallet_balance_cents += 50000  # R500
    db.session.commit()
    flash('Successfully topped up wallet with R500', 'success')
    return redirect(url_for('billing_bp.learner_dashboard'))
"""

    pattern = re.compile(r"@billing_bp\.route\('/billing/checkout/<month>'\).*?def billing_checkout\(month\):.*?return render_template\(\"program_billing/checkout_summary\.html\".*?\)", re.DOTALL)
    
    content = pattern.sub(new_checkout, content)
    
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)

    print('Updated billing routes')
