import re
routes_path = 'app/auth/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_wallet = '''    # ---------- WALLET TOKEN SUBJECTS (NO REGISTRATION FEE, USES WALLET BALANCE) ----------
    if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd", "sace"):
        mark_loss_enrollment_free(enrollment_id)
        session.pop("reg_ctx", None)
        session.pop("just_paid_subject_id", None)
        
        # We assume the 100 token trial bonus was added during user creation.
        if subject in ("cultural_fire", "culturalfire"):
            return redirect(url_for("cultural_bp.cultural_fire_router"))
        elif subject == "debtors":
            return redirect(url_for("debtors_bp.debtors_router"))'''

new_wallet = '''    # ---------- WALLET TOKEN SUBJECTS (NO REGISTRATION FEE, USES WALLET BALANCE) ----------
    if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd", "sace", "uip"):
        mark_loss_enrollment_free(enrollment_id)
        session.pop("reg_ctx", None)
        session.pop("just_paid_subject_id", None)
        
        # We assume the 100 token trial bonus was added during user creation.
        if subject in ("cultural_fire", "culturalfire"):
            return redirect(url_for("cultural_bp.cultural_fire_router"))
        elif subject == "debtors":
            return redirect(url_for("debtors_bp.debtors_router"))
        elif subject == "uip":
            # UIP has no central router yet, we can default to their org dashboard if known, 
            # or to the welcome page if org is unknown. Let's send them to bridge for now which will resolve it.
            return redirect(url_for("auth_bp.bridge_dashboard"))'''
text = text.replace(old_wallet, new_wallet)
with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
