import re

with open('app/payments/routes.py', 'r') as f:
    content = f.read()

# Replace hardcoded tokens = 100 with dynamic reading from request
content = re.sub(
    r'tokens = 100\n\s*price_cents = int\(request\.form\.get\("price_cents", 10000\)\)\n\s*zar_price_cents = int\(request\.form\.get\("zar_price_cents", 10000\)\)',
    'tokens = int(request.form.get("tokens", 100))\n    price_cents = int(request.form.get("price_cents", 10000))\n    zar_price_cents = int(request.form.get("zar_price_cents", 10000))',
    content
)

with open('app/payments/routes.py', 'w') as f:
    f.write(content)
