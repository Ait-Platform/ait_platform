with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_logic = '''    asset_total_cents = 0
    liability_total_cents = 0
    for a in accounts:
        bal = latest_balances.get(a["id"], 0)
        if a["kind"] in ("asset", "income"):
            asset_total_cents += bal
        else:
            # this covers "liability" and "expense"
            liability_total_cents += bal'''

new_logic = '''    asset_total_cents = 0
    liability_total_cents = 0
    asset_rows = []
    liability_rows = []
    for a in accounts:
        bal = latest_balances.get(a["id"], 0)
        if bal == 0:
            continue
            
        row = {"name": a["name"], "cents": bal}
        if a["kind"] in ("asset", "income"):
            asset_total_cents += bal
            asset_rows.append(row)
        else:
            # this covers "liability" and "expense"
            liability_total_cents += bal
            liability_rows.append(row)'''

text = text.replace(old_logic, new_logic)

old_return = '''        asset_total_cents=int(asset_total_cents),
        liability_total_cents=int(liability_total_cents),
        net_worth_cents=int(net_worth_cents),
        back_url=back_url,
    )'''

new_return = '''        asset_total_cents=int(asset_total_cents),
        liability_total_cents=int(liability_total_cents),
        net_worth_cents=int(net_worth_cents),
        asset_rows=asset_rows,
        liability_rows=liability_rows,
        back_url=back_url,
    )'''

text = text.replace(old_return, new_return)

with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated routes logic")
