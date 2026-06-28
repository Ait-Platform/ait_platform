import sys

new_route = '''@budget_bp.route("/reports/statement", methods=["GET"])
@login_required
def report_statement():
    back_url = _safe_next(request.args.get("next")) or url_for("budget_bp.dashboard")
    period = (request.args.get("period") or "").strip()
    if not period:
        period = datetime.now().strftime("%Y-%m")

    try:
        start_date = datetime.strptime(f"{period}-01", "%Y-%m-%d").date()
    except ValueError:
        start_date = datetime.now().date().replace(day=1)
        period = start_date.strftime("%Y-%m")

    import calendar
    last_day = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = start_date.replace(day=last_day)

    # -------- INCOME --------
    income_rows = db.session.execute(text("""
        SELECT a.name, COALESCE(SUM(l.amount_cents), 0) AS cents
          FROM bud_account a
          LEFT JOIN bud_ledger l ON a.id = l.account_id 
                                AND l.txn_date BETWEEN :s AND :e
         WHERE a.user_id = :uid
           AND a.kind = 'income'
           AND COALESCE(a.is_hidden,false) = false
         GROUP BY a.name
         ORDER BY a.name
    """), {
        "uid": current_user.id,
        "s": start_date.strftime("%Y-%m-%d"),
        "e": end_date.strftime("%Y-%m-%d"),
    }).mappings().all()

    # -------- EXPENSES --------
    expense_rows = db.session.execute(text("""
        SELECT a.name, COALESCE(SUM(l.amount_cents), 0) AS cents
          FROM bud_account a
          LEFT JOIN bud_ledger l ON a.id = l.account_id 
                                AND l.txn_date BETWEEN :s AND :e
         WHERE a.user_id = :uid
           AND a.kind IN ('expense', 'liability')
           AND COALESCE(a.is_hidden,false) = false
         GROUP BY a.name
         ORDER BY a.name
    """), {
        "uid": current_user.id,
        "s": start_date.strftime("%Y-%m-%d"),
        "e": end_date.strftime("%Y-%m-%d"),
    }).mappings().all()

    income_total_cents  = sum(r["cents"] or 0 for r in income_rows)
    expense_total_cents = sum(r["cents"] or 0 for r in expense_rows)
    net_cents = income_total_cents - expense_total_cents

    # -------- BALANCE SHEET --------
    # Fetch all active accounts
    accounts = db.session.execute(text("""
        SELECT id, name, kind
          FROM bud_account
         WHERE user_id = :uid AND is_active = 1 AND COALESCE(is_hidden,false) = false
         ORDER BY kind, name
    """), {"uid": current_user.id}).mappings().all()

    # Fetch snapshots
    snapshots = db.session.execute(text("""
        SELECT account_id, balance_cents
          FROM bud_snapshot
         WHERE user_id = :uid
         ORDER BY as_at DESC
    """), {"uid": current_user.id}).mappings().all()
    
    latest_balances = {}
    for s in snapshots:
        if s["account_id"] not in latest_balances:
            latest_balances[s["account_id"]] = s["balance_cents"]

    asset_rows = []
    liability_rows = []

    for a in accounts:
        bal = latest_balances.get(a["id"], 0)
        row = {"name": a["name"], "balance_cents": bal}
        if a["kind"] in ("asset", "income"):
            asset_rows.append(row)
        else:
            liability_rows.append(row)

    asset_total_cents = sum(r["balance_cents"] for r in asset_rows)
    liability_total_cents = sum(r["balance_cents"] for r in liability_rows)
    net_worth_cents = asset_total_cents - liability_total_cents

    return render_template(
        "program_budget/report_statement.html",
        period=period,
        income_rows=income_rows,
        expense_rows=expense_rows,
        income_total_cents=int(income_total_cents),
        expense_total_cents=int(expense_total_cents),
        net_cents=int(net_cents),
        asset_rows=asset_rows,
        liability_rows=liability_rows,
        asset_total_cents=asset_total_cents,
        liability_total_cents=liability_total_cents,
        net_worth_cents=net_worth_cents,
        back_url=back_url,
    )

'''

with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
    f.writelines(lines[:449])
    f.write(new_route)
    f.writelines(lines[570:])

print('Successfully replaced lines 450-570')
