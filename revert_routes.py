import re

with open('app/payments/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Revert to pulling from enrollment
new_content = re.sub(
    r'# Rule: 100 Tokens = 100 Local Currency units \(10000 cents\)\s*if enrollment\.local_currency and enrollment\.local_amount_cents:\s*currency = enrollment\.local_currency\s*price_cents = 10000\s*if currency == "ZAR":\s*zar_price_cents = 10000\s*elif enrollment\.zar_amount_cents:\s*exchange_rate = enrollment\.zar_amount_cents / enrollment\.local_amount_cents\s*zar_price_cents = int\(price_cents \* exchange_rate\)\s*else:\s*zar_price_cents = 10000',
    '''# Pull price directly from UserEnrollment
        if enrollment.local_currency and enrollment.local_amount_cents:
            currency = enrollment.local_currency
            price_cents = enrollment.local_amount_cents
            zar_price_cents = enrollment.zar_amount_cents or 10000''',
    content
)

new_content = re.sub(
    r'if quote:\s*currency = quote\.local_currency or "ZAR"\s*price_cents = 10000\s*if currency == "ZAR":\s*zar_price_cents = 10000\s*elif quote\.local_amount_cents and quote\.zar_amount_cents:\s*exchange_rate = quote\.zar_amount_cents / quote\.local_amount_cents\s*zar_price_cents = int\(price_cents \* exchange_rate\)\s*else:\s*zar_price_cents = 10000',
    '''if quote:
                currency = quote.local_currency or "ZAR"
                price_cents = quote.local_amount_cents or 10000
                zar_price_cents = quote.zar_amount_cents or 10000''',
    new_content
)

with open('app/payments/routes.py', 'w', encoding='utf-8') as f:
    f.write(new_content)
