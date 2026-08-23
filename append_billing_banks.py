with open('app/program_billing/routes.py', 'a', encoding='utf-8') as f:
    f.write('''
@billing_bp.route("/manager/bank_accounts/add", methods=["POST"])
@login_required
def add_bank_account():
    from app.models.debtors import BusinessBankAccount
    bank_name = request.form.get("bank_name")
    account_name = request.form.get("account_name")
    bsb_branch = request.form.get("bsb_branch")
    account_number = request.form.get("account_number")
    swift_code = request.form.get("swift_code")
    raw_details = request.form.get("raw_details")

    # If first account, make it default
    existing = BusinessBankAccount.query.filter_by(user_id=current_user.id).count()
    is_default = (existing == 0)

    try:
        new_account = BusinessBankAccount(
            user_id=current_user.id,
            bank_name=bank_name,
            account_name=account_name,
            bsb_branch=bsb_branch,
            account_number=account_number,
            swift_code=swift_code,
            raw_details=raw_details,
            is_default=is_default
        )
        db.session.add(new_account)
        db.session.commit()
        flash("Bank account added successfully.", "success")
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Failed to add bank account: {e}")
        flash("Failed to add bank account.", "error")

    return redirect(url_for('billing_bp.bank_accounts'))


@billing_bp.route("/manager/bank_accounts/set_default/<int:account_id>", methods=["POST"])
@login_required
def set_default_bank_account(account_id):
    from app.models.debtors import BusinessBankAccount
    account = BusinessBankAccount.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    
    try:
        BusinessBankAccount.query.filter_by(user_id=current_user.id).update({"is_default": False})
        account.is_default = True
        db.session.commit()
        flash("Default bank account updated.", "success")
    except Exception as e:
        db.session.rollback()
        flash("Failed to update default account.", "error")
        
    return redirect(url_for('billing_bp.bank_accounts'))


@billing_bp.route("/manager/bank_accounts/delete/<int:account_id>", methods=["POST"])
@login_required
def delete_bank_account(account_id):
    from app.models.debtors import BusinessBankAccount
    account = BusinessBankAccount.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    
    try:
        was_default = account.is_default
        db.session.delete(account)
        db.session.commit()
        
        # If we deleted the default, randomly assign a new default if accounts exist
        if was_default:
            first_remaining = BusinessBankAccount.query.filter_by(user_id=current_user.id).first()
            if first_remaining:
                first_remaining.is_default = True
                db.session.commit()
                
        flash("Bank account deleted.", "success")
    except Exception as e:
        db.session.rollback()
        flash("Failed to delete account.", "error")
        
    return redirect(url_for('billing_bp.bank_accounts'))
''')
