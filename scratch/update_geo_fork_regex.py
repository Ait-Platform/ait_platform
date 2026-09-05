import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# I will find the WALLET TOKEN SUBJECTS section and replace it.
pattern = r'# ---------- WALLET TOKEN SUBJECTS.*?if subject in \("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd", "sace", "sace_evaluator", "sace_facilitator", "sace_participant", "sace_hub"\):'
new_block = """# ---------- WALLET TOKEN SUBJECTS (NO REGISTRATION FEE, USES WALLET BALANCE) ----------
    ctx_country = (session.get("reg_ctx", {}).get("country_code") or request.headers.get("CF-IPCountry", "ZA")).upper()
    is_sace = subject in ("sace", "sace_hub", "sace_evaluator", "sace_facilitator", "sace_participant")
    
    is_free = False
    if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd"):
        is_free = True
    elif is_sace and ctx_country == "ZA":
        is_free = True

    if is_free:"""

content = re.sub(pattern, new_block, content, flags=re.DOTALL)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated auth routing with regex")
