import re

with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement_dash = '''@budget_bp.get("/dashboard")
@login_required
def dashboard():
    from app.models.auth import AitTokenWallet
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    return render_template("program_budget/dashboard.html", wallet=wallet)'''

content = re.sub(
    r"@budget_bp\.get\(\"/dashboard\"\)\s*@login_required\s*def dashboard\(\):\s*return render_template\(\"program_budget/dashboard\.html\"\)",
    replacement_dash,
    content,
    flags=re.DOTALL
)

with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
