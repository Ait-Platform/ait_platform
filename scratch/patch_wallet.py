import re

file_path = 'app/services/users.py'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_wallet = '''    # Create AitTokenWallet with 100 tokens for new users (Registration Trial Bonus)
    db.session.execute(
        sa_text("""
            INSERT INTO ait_token_wallet (user_id, balance, created_at)
            VALUES (:user_id, 100, CURRENT_TIMESTAMP)
        """),
        {"user_id": new_id}
    )
    db.session.flush()
    
    wallet_id = int(db.session.execute(
        sa_text('SELECT id FROM ait_token_wallet WHERE user_id = :u'),
        {"u": new_id}
    ).scalar())

    db.session.execute(
        sa_text("""
            INSERT INTO ait_token_transaction (wallet_id, amount, description, created_at)
            VALUES (:wallet_id, 100, 'Registration Trial Bonus', CURRENT_TIMESTAMP)
        """),
        {"wallet_id": wallet_id}
    )'''

new_wallet = '''    # Create AitTokenWallet with 100 tokens for new users (Registration Trial Bonus)
    # Exclude SACE Endorsement users from receiving tokens/wallets
    subject = (ctx.get("subject") or "").strip().lower()
    if not subject.startswith("sace"):
        db.session.execute(
            sa_text("""
                INSERT INTO ait_token_wallet (user_id, balance, created_at)
                VALUES (:user_id, 100, CURRENT_TIMESTAMP)
            """),
            {"user_id": new_id}
        )
        db.session.flush()
        
        wallet_id = int(db.session.execute(
            sa_text('SELECT id FROM ait_token_wallet WHERE user_id = :u'),
            {"u": new_id}
        ).scalar())

        db.session.execute(
            sa_text("""
                INSERT INTO ait_token_transaction (wallet_id, amount, description, created_at)
                VALUES (:wallet_id, 100, 'Registration Trial Bonus', CURRENT_TIMESTAMP)
            """),
            {"wallet_id": wallet_id}
        )'''

text = text.replace(old_wallet, new_wallet)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
