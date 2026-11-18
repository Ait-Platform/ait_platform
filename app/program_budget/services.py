from datetime import datetime
import calendar
from sqlalchemy import text
from app.extensions import db

def budget_summary(user_id: int, period: str = None) -> dict:
    start_date = None
    end_date = None
    if period:
        try:
            start_date = datetime.strptime(f"{period}-01", "%Y-%m-%d").date()
            last_day = calendar.monthrange(start_date.year, start_date.month)[1]
            end_date = start_date.replace(day=last_day)
        except (ValueError, TypeError):
            start_date = None
            end_date = None

    opening_balance_cents = 0

    if start_date:
        prior_rows = db.session.execute(text("""
            SELECT l.amount_cents, a.kind
              FROM bud_ledger l
              JOIN bud_account a ON a.id = l.account_id AND a.user_id = :uid
             WHERE l.user_id = :uid
               AND l.source = 'external'
               AND l.txn_date < :start_date
             ORDER BY l.txn_date ASC, l.id ASC
        """), {"uid": int(user_id), "start_date": start_date}).mappings().all()

        for r in prior_rows:
            amt = r["amount_cents"] or 0
            if r["kind"] == "income":
                opening_balance_cents += amt
            elif r["kind"] in ("expense", "liability"):
                opening_balance_cents -= amt
            else:
                opening_balance_cents += amt
    else:
        has_records = db.session.execute(text("""
            SELECT 1 FROM bud_ledger l
              JOIN bud_account a ON a.id = l.account_id AND a.user_id = :uid
             WHERE l.user_id = :uid AND l.source = 'external'
             LIMIT 1
        """), {"uid": int(user_id)}).scalar()
        if not has_records:
            return {
                "opening_balance_cents": 0,
                "current_balance_cents": 0,
                "closing_balance_cents": 0,
                "income_cents": 0,
                "income_total_cents": 0,
                "expense_cents": 0,
                "expense_total_cents": 0,
                "net_cents": 0,
                "arrears_cents": 0,
                "amount_due_cents": 0,
                "period": None,
            }

    query_sql = """
        SELECT l.amount_cents, a.kind, l.txn_date
          FROM bud_ledger l
          JOIN bud_account a ON a.id = l.account_id AND a.user_id = :uid
         WHERE l.user_id = :uid
           AND l.source = 'external'
    """
    params = {"uid": int(user_id)}
    if start_date and end_date:
        query_sql += " AND l.txn_date BETWEEN :start_date AND :end_date"
        params["start_date"] = start_date
        params["end_date"] = end_date
    query_sql += " ORDER BY l.txn_date ASC, l.id ASC"

    rows = db.session.execute(text(query_sql), params).mappings().all()

    if not rows and not start_date:
        return {
            "opening_balance_cents": 0,
            "current_balance_cents": 0,
            "closing_balance_cents": 0,
            "income_cents": 0,
            "income_total_cents": 0,
            "expense_cents": 0,
            "expense_total_cents": 0,
            "net_cents": 0,
            "arrears_cents": 0,
            "amount_due_cents": 0,
            "period": period,
        }

    income_total_cents = 0
    expense_total_cents = 0

    for r in rows:
        amt = r["amount_cents"] or 0
        if r["kind"] == "income":
            income_total_cents += amt
        elif r["kind"] in ("expense", "liability"):
            expense_total_cents += amt
        else:
            if amt >= 0:
                income_total_cents += amt
            else:
                expense_total_cents += abs(amt)

    net_cents = income_total_cents - expense_total_cents
    current_balance_cents = opening_balance_cents + net_cents
    arrears_cents = max(0, -current_balance_cents)

    return {
        "opening_balance_cents": int(opening_balance_cents),
        "current_balance_cents": int(current_balance_cents),
        "closing_balance_cents": int(current_balance_cents),
        "income_cents": int(income_total_cents),
        "income_total_cents": int(income_total_cents),
        "expense_cents": int(expense_total_cents),
        "expense_total_cents": int(expense_total_cents),
        "net_cents": int(net_cents),
        "arrears_cents": int(arrears_cents),
        "amount_due_cents": int(arrears_cents),
        "period": period,
    }
