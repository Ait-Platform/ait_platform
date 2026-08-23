import re

with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''@budget_bp.route("/ledger/add", methods=["POST"])
@login_required
def ledger_add():
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    
    account_id = (request.form.get("account_id") or "").strip()
    txn_date = (request.form.get("txn_date") or "").strip()
    amount = (request.form.get("amount") or "").strip()

    if not (account_id.isdigit() and txn_date and amount):
        flash("Please choose an account, date, and amount.", "warning")
        return redirect(url_for("budget_bp.ledger"))

    try:
        cents = int(round(float(amount.replace(",", "")) * 100))
    except Exception:
        flash("Invalid amount.", "warning")
        return redirect(url_for("budget_bp.ledger"))
        
    # --- WALLET TOKEN CHECK ---
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    token_cost = 10
    
    if not wallet or wallet.balance < token_cost:
        flash(f"You need at least {token_cost} tokens in your wallet to capture an entry.", "error")
        return redirect(url_for("payment_bp.wallet_topup", subject_slug="budget"))

    # 🔹 Always show a consistent meaning in the ledger table
    description = "Paid"

    try:
        db.session.execute(text("""
            INSERT INTO bud_ledger (user_id, account_id, txn_date, description, amount_cents)
            VALUES (:uid, :aid, :d, :desc, :c)
        """), {
            "uid": current_user.id,
            "aid": int(account_id),
            "d": txn_date,
            "desc": description,
            "c": int(cents),
        })
        
        # Deduct tokens
        wallet.balance -= token_cost
        txn = AitTokenTransaction(
            wallet_id=wallet.id,
            amount=-token_cost,
            description=f"Captured Cashbook Entry (Account ID {account_id})"
        )
        db.session.add(txn)
        
        db.session.commit()
        flash("Payment added. 10 tokens deducted.", "success")'''

content = re.sub(
    r"@budget_bp\.route\(\"/ledger/add\", methods=\[\"POST\"\]\)\s*@login_required\s*def ledger_add\(\):\s*account_id = \(request\.form\.get\(\"account_id\"\) or \"\"\)\.strip\(\).*?description = \"Paid\"\s*try:\s*db\.session\.execute\(text\(\"\"\"\s*INSERT INTO bud_ledger \(user_id, account_id, txn_date, description, amount_cents\)\s*VALUES \(:uid, :aid, :d, :desc, :c\)\s*\"\"\"\), \{\s*\"uid\": current_user\.id,\s*\"aid\": int\(account_id\),\s*\"d\": txn_date,\s*\"desc\": description,\s*\"c\": int\(cents\),\s*\}\)\s*db\.session\.commit\(\)\s*flash\(\"Payment added\.\", \"success\"\)",
    replacement,
    content,
    flags=re.DOTALL
)

with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
