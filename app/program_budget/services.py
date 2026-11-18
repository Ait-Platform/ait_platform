from app.models.budget import BudLedger
from app.extensions import db

def budget_summary(user_id: int) -> dict:
    q = BudLedger.query.filter_by(user_id=user_id, source="external").order_by(BudLedger.id.asc())
    first = q.first()
    last = BudLedger.query.filter_by(user_id=user_id, source="external").order_by(BudLedger.id.desc()).first()



    if not first or not last:
        return {
            "opening_balance_cents": 0,
            "current_balance_cents": 0,
            "arrears_cents": 0,
            "amount_due_cents": 0,
        }

    opening = first.balance_cents - first.amount_cents
    current = last.balance_cents
    arrears = max(0, -current)

    return {
        "opening_balance_cents": opening,
        "current_balance_cents": current,
        "arrears_cents": arrears,
        "amount_due_cents": arrears,
    }
