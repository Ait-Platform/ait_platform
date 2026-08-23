import re

with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement_price = '''@budget_bp.route('/price')
@login_required
def price_page():
    # Redirect to wallet topup
    return redirect(url_for('payment_bp.wallet_topup', subject_slug='budget'))'''

content = re.sub(
    r"@budget_bp\.route\('/price'\)\s*def price_page\(\):.*?return render_template\('program_budget/price\.html', \*\*price_ctx\)",
    replacement_price,
    content,
    flags=re.DOTALL
)

replacement_dash = '''@budget_bp.route("/dashboard", methods=["GET"])
@login_required
def dashboard():
    from app.models.auth import AitTokenWallet
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    return render_template("program_budget/dashboard.html", wallet=wallet)'''

content = re.sub(
    r"@budget_bp\.route\(\"/dashboard\", methods=\[\"GET\"\]\)\s*@login_required\s*def dashboard\(\):\s*return render_template\(\"program_budget/dashboard\.html\"\)",
    replacement_dash,
    content,
    flags=re.DOTALL
)

with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
