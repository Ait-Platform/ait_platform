with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

start = text.find('def report_statement()')
end = text.find('return render_template(', start)

if start != -1 and end != -1:
    net_worth_code = '''
    # -------- NET WORTH --------
    accounts = db.session.execute(text("""
        SELECT id, kind FROM bud_account
         WHERE user_id = :uid AND is_active = 1 AND COALESCE(is_hidden,false) = false
           AND kind IN ('asset', 'liability')
    """), {"uid": current_user.id}).mappings().all()

    snapshots = db.session.execute(text("""
        SELECT account_id, balance_cents FROM bud_snapshot
         WHERE user_id = :uid ORDER BY as_at DESC
    """), {"uid": current_user.id}).mappings().all()
    
    latest_balances = {}
    for s in snapshots:
        if s["account_id"] not in latest_balances:
            latest_balances[s["account_id"]] = s["balance_cents"]

    net_worth_cents = 0
    for a in accounts:
        bal = latest_balances.get(a["id"], 0)
        if a["kind"] == "asset":
            net_worth_cents += bal
        elif a["kind"] == "liability":
            net_worth_cents -= bal

'''
    text = text[:end] + net_worth_code + text[end:]
    
    # Also pass it to render_template
    text = text.replace(
        '        net_cents=int(net_cents),\n        back_url=back_url,\n    )',
        '        net_cents=int(net_cents),\n        net_worth_cents=int(net_worth_cents),\n        back_url=back_url,\n    )'
    )

    with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print('Inserted net worth logic')
else:
    print('Failed to find markers')
