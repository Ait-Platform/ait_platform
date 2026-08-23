import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

bad_block = '''@mechanic_bp.route("/mechanic/fix_wallet", methods=["GET"])
@login_required
def fix_wallet():
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    if not wallet:
        wallet = AitTokenWallet(user_id=current_user.id, balance=0)
        db.session.add(wallet)
        db.session.flush()
    wallet.balance += 200
    tx = AitTokenTransaction(wallet_id=wallet.id, amount=200, description="Manual fix for missing Render tokens")
    db.session.add(tx)
    db.session.commit()
    flash("200 Tokens injected successfully into Render DB!", "success")
    return redirect(url_for('mechanic_bp.mechanic_dashboard'))'''

# Remove from top
content = content.replace(bad_block + '\n', '')

# Insert it after rom . import mechanic_bp
content = content.replace('from . import mechanic_bp', 'from . import mechanic_bp\n\n' + bad_block)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
