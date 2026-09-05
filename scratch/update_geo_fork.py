import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the current hardcoded free list
old_logic = """    # ---------- WALLET TOKEN SUBJECTS (NO REGISTRATION FEE, USES WALLET BALANCE) ----------
    if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd", "sace", "sace_evaluator", "sace_facilitator", "sace_participant", "sace_hub"):
        mark_loss_enrollment_free(enrollment_id)
        session.pop("reg_ctx", None)
        session.pop("just_paid_subject_id", None)
        
        # We assume the 100 token trial bonus was added during user creation.
        if subject in ("cultural_fire", "culturalfire"):
            return redirect(url_for("cultural_bp.cultural_fire_router"))
        elif subject == "debtors":
            return redirect(url_for("debtors_bp.debtors_router"))
        elif subject == "mechanic":
            return redirect(url_for("mechanic_bp.mechanic_dashboard"))
        elif subject == "cptd" or "sace" in subject:
            return redirect(url_for("sace_bp.catalog"))"""

new_logic = """    # ---------- WALLET TOKEN SUBJECTS (NO REGISTRATION FEE, USES WALLET BALANCE) ----------
    ctx_country = (session.get("reg_ctx", {}).get("country_code") or request.headers.get("CF-IPCountry", "ZA")).upper()
    is_sace = subject in ("sace", "sace_hub", "sace_evaluator", "sace_facilitator", "sace_participant")
    
    is_free = False
    if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd"):
        is_free = True
    elif is_sace and ctx_country == "ZA":
        is_free = True

    if is_free:
        mark_loss_enrollment_free(enrollment_id)
        session.pop("reg_ctx", None)
        session.pop("just_paid_subject_id", None)
        
        # We assume the 100 token trial bonus was added during user creation.
        if subject in ("cultural_fire", "culturalfire"):
            return redirect(url_for("cultural_bp.cultural_fire_router"))
        elif subject == "debtors":
            return redirect(url_for("debtors_bp.debtors_router"))
        elif subject == "mechanic":
            return redirect(url_for("mechanic_bp.mechanic_dashboard"))
        elif subject == "cptd" or "sace" in subject:
            return redirect(url_for("sace_bp.catalog"))"""

content = content.replace(old_logic, new_logic)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Implemented Geo-Price Fork")
