import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update is_trial to always be True
replacement_trial_old = '''    main_prop = props[0] if props else None
    from datetime import datetime
    is_trial = main_prop.trial_ends_at and main_prop.trial_ends_at > datetime.utcnow() if main_prop else False'''

replacement_trial_new = '''    main_prop = props[0] if props else None
    from datetime import datetime
    # Hardcode trial to True for testing
    is_trial = True'''

text = text.replace(replacement_trial_old, replacement_trial_new)

# 2. Add the missing billing_unlock route
unlock_route = '''
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
'''

text += unlock_route

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated routes with billing_unlock and hardcoded trial')
