import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove sace from the free list entirely so they hit the paystack gate
old_free = """    # ---------- WALLET TOKEN SUBJECTS (NO REGISTRATION FEE, USES WALLET BALANCE) ----------
    ctx_country = (session.get("reg_ctx", {}).get("country_code") or request.headers.get("CF-IPCountry", "ZA")).upper()
    is_sace = subject in ("sace", "sace_hub", "sace_evaluator", "sace_facilitator", "sace_participant")
    
    is_free = False
    if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd"):
        is_free = True
    elif is_sace and ctx_country == "ZA":
        is_free = True

    if is_free:"""

new_free = """    # ---------- WALLET TOKEN SUBJECTS (NO REGISTRATION FEE, USES WALLET BALANCE) ----------
    if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd"):"""

content = content.replace(old_free, new_free)

# 2. Update post-login routing
# Look for: elif slug == 'cptd' or 'sace' in slug: return redirect(url_for("sace_bp.catalog"))
old_route = """        elif subject == "cptd" or "sace" in subject:
            return redirect(url_for("sace_bp.catalog"))"""
new_route = """        elif subject == "cptd":
            return redirect(url_for("sace_bp.catalog"))
        elif subject.startswith("sace_"):
            activity = subject.replace("sace_", "")
            return redirect(url_for("sace_bp.selection_hub", activity_slug=activity))"""

content = content.replace(old_route, new_route)

old_route2 = """        elif slug == 'cptd' or 'sace' in slug:
            return redirect(url_for('sace_bp.catalog'))"""
new_route2 = """        elif slug == 'cptd':
            return redirect(url_for('sace_bp.catalog'))
        elif slug.startswith('sace_'):
            activity = slug.replace('sace_', '')
            return redirect(url_for('sace_bp.selection_hub', activity_slug=activity))"""

content = content.replace(old_route2, new_route2)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Removed geo-fork and updated dynamic activity routing")
