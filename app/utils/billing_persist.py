# app/utils/billing_persist.py

from sqlalchemy import text
from app.extensions import db
from app.utils.billing_metsoa import get_electricity_rate_for_month
from app.utils.billing_metsoa_builder import calc_ws_sd_totals


def _month_bounds(month_str):
    # month_str = 'YYYY-MM'
    first = f"{month_str}-01"
    # last day via SQLite date math when we need it inline; here just return first
    return first

def _upsert_metsoa_row(conn, payload):
    """
    payload keys (subset):
      tenant_id, meter_id, month, utility_type,
      prev_date, prev_read, curr_date, curr_read, days, consumption,
      elec_rate, elec_due, ws_amount, sd_amount, water_amount, total_due
    """
    sql = """
    INSERT INTO bil_metsoa_meter_month (
      tenant_id, meter_id, month, utility_type,
      prev_date, prev_read, curr_date, curr_read, days, consumption,
      elec_rate, elec_due, ws_amount, sd_amount, water_amount, total_due
    )
    VALUES (
      :tenant_id, :meter_id, :month, :utility_type,
      :prev_date, :prev_read, :curr_date, :curr_read, :days, :consumption,
      :elec_rate, :elec_due, :ws_amount, :sd_amount, :water_amount, :total_due
    )
    ON CONFLICT(tenant_id, meter_id, month) DO UPDATE SET
      utility_type=excluded.utility_type,
      prev_date=excluded.prev_date,
      prev_read=excluded.prev_read,
      curr_date=excluded.curr_date,
      curr_read=excluded.curr_read,
      days=excluded.days,
      consumption=excluded.consumption,
      elec_rate=excluded.elec_rate,
      elec_due=excluded.elec_due,
      ws_amount=excluded.ws_amount,
      sd_amount=excluded.sd_amount,
      water_amount=excluded.water_amount,
      total_due=excluded.total_due
    """
    res = conn.execute(text(sql), payload)
    # fetch id (insert or update). Try to get id by SELECT after upsert.
    row = conn.execute(text("""
      SELECT id FROM bil_metsoa_meter_month
      WHERE tenant_id=:tenant_id AND meter_id=:meter_id AND month=:month
    """), {"tenant_id": payload["tenant_id"], "meter_id": payload["meter_id"], "month": payload["month"]}).first()
    return row.id if row else None

def _replace_breakdown(conn, metsoa_id, lines):
    """
    lines: list of dicts with keys: bucket, cons, rate, amount, sort_order
    """
    conn.execute(text("DELETE FROM bil_metsoa_breakdown WHERE metsoa_id=:id"), {"id": metsoa_id})
    if not lines:
        return
    conn.execute(text("""
      INSERT INTO bil_metsoa_breakdown (metsoa_id, bucket, cons, rate, amount, sort_order)
      VALUES (:metsoa_id, :bucket, :cons, :rate, :amount, :sort_order)
    """), [{"metsoa_id": metsoa_id, **ln} for ln in lines])

def _post_tenant_charge(conn, tenant_id, month_str, amount, desc="Due to Metro"):
    # Signed convention: charges are positive
    if not amount:
        return
    conn.execute(text("""
      INSERT INTO bil_tenant_ledger (tenant_id, txn_date, month, description, kind, amount, ref)
      VALUES (:tenant_id, date('now'), :month, :desc, 'charge', :amount, :ref)
    """), {
        "tenant_id": tenant_id, "month": month_str, "desc": desc, "amount": float(amount),
        "ref": f"METSOA {month_str}"
    })

def _get_consumption_rows(conn, tenant_id, month_str):
    # You already use this shape everywhere
    return conn.execute(text("""
      SELECT
        c.meter_id                         AS meter_id,
        COALESCE(m.meter_number, CAST(m.id AS TEXT)) AS meter_label,
        LOWER(m.utility_type)              AS utility_type,
        c.last_date                        AS prev_date,
        c.last_read                        AS prev_value,
        c.new_date                         AS curr_date,
        c.new_read                         AS curr_value,
        c.days                             AS days,
        c.consumption                      AS consumption
      FROM bil_consumption c
      JOIN bil_meter m ON m.id = c.meter_id
      WHERE c.month = :month AND c.tenant_id = :tid
      ORDER BY CASE WHEN LOWER(m.utility_type) LIKE 'elec%%' THEN 0 ELSE 1 END,
               m.meter_number
    """), {"month": month_str, "tid": tenant_id}).mappings().all()

def commit_metsoa_for_month(tenant_id, month_str):
    """
    Recompute and PERSIST everything for this tenant+month:
      - bil_metsoa_meter_month (per meter record)
      - bil_metsoa_breakdown (tiers/surcharges/fixed)
      - bil_tenant_ledger single charge "Due to Metro — YYYY-MM"
    Uses your existing helpers: calc_ws_sd_totals, get_electricity_rate_for_month
    """
    first_day = _month_bounds(month_str)

    # 1) pull base rows
    base_rows = _get_consumption_rows(db.session, tenant_id, month_str)

    # 2) one-time elec rate
    elec_rate = get_electricity_rate_for_month(month_str) or 0.0

    grand_total = 0.0
    per_meter_totals = []  # (meter_id, total_due) to optionally use later

    for r in base_rows:
        util = (r["utility_type"] or "").strip().lower()
        cons = int(r["consumption"] or 0)
        days = int(r["days"] or 0)

        # Default payload
        payload = {
            "tenant_id":   tenant_id,
            "meter_id":    r["meter_id"],
            "month":       month_str,
            "utility_type": util,
            "prev_date":   r["prev_date"],
            "prev_read":   r["prev_value"],
            "curr_date":   r["curr_date"],
            "curr_read":   r["curr_value"],
            "days":        days,
            "consumption": cons,
            "elec_rate":   None,
            "elec_due":    None,
            "ws_amount":   None,
            "sd_amount":   None,
            "water_amount": None,
            "total_due":   0.0,
        }

        breakdown_lines = []

        if util.startswith("elec"):
            due = round(cons * elec_rate, 2)
            payload["elec_rate"] = elec_rate
            payload["elec_due"]  = due
            payload["total_due"] = due
            # breakdown (single line helpful for audit)
            breakdown_lines.append({
                "bucket": "elec",
                "cons": float(cons),
                "rate": float(elec_rate),
                "amount": float(due),
                "sort_order": 10,
            })
        else:
            # Water path via the MAP + tariff
            totals = calc_ws_sd_totals(
                meter_id=r["meter_id"],
                month_str=month_str,
                consumption_kl=cons,
                days=days
            ) or {}

            ws_amt = float(totals.get("ws_amount") or 0.0)
            sd_amt = float(totals.get("sd_amount") or 0.0)
            w_amt  = float(totals.get("water_cost") or (ws_amt + sd_amt))

            payload["ws_amount"]    = ws_amt
            payload["sd_amount"]    = sd_amt
            payload["water_amount"] = w_amt
            payload["total_due"]    = w_amt

            # Optional: if your calc exposes breakdown arrays, add them; otherwise keep simple
            for i, ln in enumerate(totals.get("ws_lines", []), start=1):
                breakdown_lines.append({
                    "bucket": f"ws_{ln.get('code','line')}",
                    "cons": float(ln.get("cons") or 0),
                    "rate": float(ln.get("rate") or 0),
                    "amount": float(ln.get("amount") or 0),
                    "sort_order": 100 + i
                })
            for j, ln in enumerate(totals.get("sd_lines", []), start=1):
                breakdown_lines.append({
                    "bucket": f"sd_{ln.get('code','line')}",
                    "cons": float(ln.get("cons") or 0),
                    "rate": float(ln.get("rate") or 0),
                    "amount": float(ln.get("amount") or 0),
                    "sort_order": 200 + j
                })

        # 3) persist header + breakdown
        metsoa_id = _upsert_metsoa_row(db.session, payload)
        _replace_breakdown(db.session, metsoa_id, breakdown_lines)

        grand_total += float(payload["total_due"] or 0.0)
        per_meter_totals.append((r["meter_id"], float(payload["total_due"] or 0.0)))

    # 4) post a single ledger charge for the month
    _post_tenant_charge(db.session, tenant_id, month_str, grand_total, desc=f"Due to Metro — {month_str}")

    db.session.commit()
    return {"total_posted": round(grand_total, 2), "meters": per_meter_totals}
