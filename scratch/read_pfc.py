with open('app/payments/pricing.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        if "def price_for_country(" in line:
            print("".join(lines[i:i+40]))
            break
