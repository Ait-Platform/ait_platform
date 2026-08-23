import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

new_routes = '''

from app.models.debtors import BusinessBankAccount
from app.program_debtors.forms import BankAccountForm

@mechanic_bp.route("/mechanic/bank_accounts")
@login_required
def bank_accounts():
    bank_accounts = BusinessBankAccount.query.filter_by(user_id=current_user.id).order_by(BusinessBankAccount.created_at.desc()).all()
    bank_form = BankAccountForm()
    return render_template("program_mechanic/bank_accounts.html", bank_accounts=bank_accounts, bank_form=bank_form)


@mechanic_bp.route("/mechanic/add_bank_account", methods=["POST"])
@login_required
def add_bank_account():
    form = BankAccountForm()
    if form.validate_on_submit():
        if form.is_default.data:
            BusinessBankAccount.query.filter_by(user_id=current_user.id).update({'is_default': False})

        new_acc = BusinessBankAccount(
            user_id=current_user.id,
            bank_name=form.bank_name.data,
            account_name=form.account_name.data,
            account_number=form.account_number.data,
            bsb_branch=form.bsb_branch.data,
            swift_code=form.swift_code.data,
            is_default=form.is_default.data
        )

        if BusinessBankAccount.query.filter_by(user_id=current_user.id).count() == 0:
            new_acc.is_default = True

        db.session.add(new_acc)
        db.session.commit()
        flash("Bank account added successfully.", "success")
    return redirect(url_for("mechanic_bp.bank_accounts"))


@mechanic_bp.route("/mechanic/bank_account/<int:account_id>/set_default", methods=["POST"])
@login_required
def set_default_bank_account(account_id):
    acc = BusinessBankAccount.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    BusinessBankAccount.query.filter_by(user_id=current_user.id).update({'is_default': False})
    acc.is_default = True
    db.session.commit()
    flash("Default bank account updated.", "success")
    return redirect(url_for("mechanic_bp.bank_accounts"))


@mechanic_bp.route("/mechanic/bank_account/<int:account_id>/delete", methods=["POST"])
@login_required
def delete_bank_account(account_id):
    acc = BusinessBankAccount.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    db.session.delete(acc)
    db.session.commit()
    flash("Bank account deleted.", "info")
    return redirect(url_for("mechanic_bp.bank_accounts"))
'''

content += new_routes

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
