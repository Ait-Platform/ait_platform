# utils/billing_metsoa.py
# Helpers for METSOA Page 1 & 2. No imports shown—use your project’s usual import style.

from sqlalchemy import text
from app.extensions import db

def _row_to_dict(r):
    return {k: getattr(r, k) if hasattr(r, k) else r[k] for k in r.keys()}

def _get_tariffs(utility_type: str, month_str: str):
    """
    Returns tariff rows for a utility, effective for month_str (YYYY-MM),
    ordered by block_start (for tiered pricing).
    Columns expected on bil_tariff:
      utility_type, code, description, rate, block_start, block_end, effective_date, reduction_factor (nullable), unit
    """
    sql = text("""
        SELECT utility_type, code, description, rate, block_start, block_end,
               effective_date, COALESCE(reduction_factor, 1.0) AS reduction_factor, unit
        FROM bil_tariff
        WHERE utility_type = :u
          AND date(substr(:m,1,4) || '-' || substr(:m,6,2) || '-01') >= date(effective_date)
        ORDER BY CASE WHEN block_start IS NULL THEN 999999 ELSE block_start END
    """)
    rows = db.session.execute(sql, {"u": utility_type, "m": month_str}).fetchall()
    return [ _row_to_dict(r) for r in rows ]

def _get_tariff_by_code(code: str, month_str: str):
    sql = text("""
        SELECT utility_type, code, description, rate, block_start, block_end,
               effective_date, COALESCE(reduction_factor, 1.0) AS reduction_factor, unit
        FROM bil_tariff
        WHERE code = :c
          AND date(substr(:m,1,4) || '-' || substr(:m,6,2) || '-01') >= date(effective_date)
        ORDER BY date(effective_date) DESC
        LIMIT 1
    """)
    r = db.session.execute(sql, {"c": code, "m": month_str}).fetchone()
    return None if not r else _row_to_dict(r)

def get_electricity_rate_for_month(month_str: str):
    # Prefer explicit ElecRate by code; fallback to first electricity row.
    t = _get_tariff_by_code("ElecRate", month_str)
    if t and t.get("rate") is not None:
        return float(t["rate"])
    ts = _get_tariffs("electricity", month_str)
    return float(ts[0]["rate"]) if ts else None

def fetch_consumption_for_month(month_str: str):
    """
    Pulls all consumption rows for the month, already scoped by your upstream process.
    No tenant filter (your pipeline already writes the month+tenant set to bil_consumption).
    """
    sql = text("""
        SELECT
          c.meter_id,
          m.meter_number  AS meter_label,
          m.utility_type  AS utility_type,
          c.last_date     AS prev_date,
          c.last_read     AS prev_value,
          c.new_date      AS curr_date,
          c.new_read      AS curr_value,
          c.days          AS days,
          c.consumption   AS consumption
        FROM bil_consumption c
        JOIN bil_meter m ON m.id = c.meter_id
        WHERE c.month = :m
        ORDER BY CASE WHEN lower(m.utility_type)='electricity' THEN 0 ELSE 1 END,
                 m.meter_number
    """)
    rows = db.session.execute(sql, {"m": month_str}).fetchall()
    return [ _row_to_dict(r) for r in rows ]

def _daily_liters(cons_kL: float, days: int) -> float:
    if not days or days <= 0:
        return 0.0
    return (cons_kL * 1000.0) / float(days)

def _tier_kL_for_daily_window(daily_L: float, days: int, start_L: float, end_L: float|None) -> float:
    """
    Allocate kL to a [start_L, end_L] liters/day window for the given daily_L and days.
    If end_L is None => open-ended upper tier.
    """
    if daily_L <= start_L:
        return 0.0
    upper = daily_L if (end_L is None or daily_L < end_L) else end_L
    span_L = max(0.0, upper - start_L)
    if span_L <= 0.0:
        return 0.0
    total_L = span_L * max(0, days)
    return total_L / 1000.0  # to kL

def _price_ws_sd_tiers(cons_kL: float, days: int, month_str: str):
    """
    Returns:
      ws_lines = [{label, cons, rate, amount}], ws_total
      sd_lines = [{label, cons, rate, amount}], sd_total
    Uses bil_tariff blocks (water & sanitation) with block_start/end in L/day
    and reduction_factor for SD tiers (defaults to 1.0).
    """
    daily_L = _daily_liters(cons_kL, days)

    # WATER (WS)
    ws_tiers = _get_tariffs("water", month_str)
    ws_lines, ws_total = [], 0.0
    for t in ws_tiers:
        start_L = float(t["block_start"]) if t["block_start"] is not None else 0.0
        end_L   = float(t["block_end"]) if t["block_end"] not in (None, 0) else None
        kL = _tier_kL_for_daily_window(daily_L, days, start_L, end_L)
        if kL <= 0:
            continue
        rate = float(t["rate"])
        amt  = kL * rate
        ws_lines.append({
            "label": f"{int(start_L)}L–{('∞' if end_L is None else int(end_L))}L / {days} Days",
            "cons": round(kL, 2),
            "rate": rate,
            "amount": round(amt, 2),
        })
        ws_total += amt

    # SANITATION (SD) — apply reduction_factor per tier
    sd_tiers = _get_tariffs("sanitation", month_str)
    sd_lines, sd_total = [], 0.0
    for t in sd_tiers:
        start_L = float(t["block_start"]) if t["block_start"] is not None else 0.0
        end_L   = float(t["block_end"]) if t["block_end"] not in (None, 0) else None
        kL = _tier_kL_for_daily_window(daily_L, days, start_L, end_L)
        if kL <= 0:
            continue
        red = float(t.get("reduction_factor", 1.0)) or 1.0
        red_kL = kL * red
        rate = float(t["rate"])
        amt  = red_kL * rate
        sd_lines.append({
            "label": f"{int(start_L)}L–{('∞' if end_L is None else int(end_L))}L / {days} Days",
            "cons": round(red_kL, 2),  # show reduced kL on SD side
            "rate": rate,
            "amount": round(amt, 2),
        })
        sd_total += amt

    return ws_lines, round(ws_total, 2), sd_lines, round(sd_total, 2)

def _get_meter_charge_map(meter_id: int, month_str: str):
    """
    Returns list of charge codes for this meter that are enabled/effective in the month.
    Table expected: bil_meter_charge_map (meter_id, charge_code, is_enabled INT, effective_start DATE)
    """
    sql = text("""
        SELECT charge_code
        FROM bil_meter_charge_map
        WHERE meter_id = :mid
          AND COALESCE(is_enabled,1) = 1
          AND date(substr(:m,1,4) || '-' || substr(:m,6,2) || '-01') >= date(COALESCE(effective_start,'1900-01-01'))
        ORDER BY charge_code
    """)
    rows = db.session.execute(sql, {"mid": meter_id, "m": month_str}).fetchall()
    return [r.charge_code for r in rows]

def _fixed_lines_from_map(meter_id: int, month_str: str, cons_kL: float):
    """
    Builds two lists of fixed/consumption-based extras from the meter map:
      ws_fixed_lines, ws_fixed_total
      sd_fixed_lines, sd_fixed_total
    Rule: 'WSSurcharge' and 'SDSurcharge' are cons-based (cons × rate; zero if cons=0).
          Others (WaterLossLevy, RefuseBin, MgmtFee, RatesZero…) are flat monthly charges (amount=rate).
    """
    codes = _get_meter_charge_map(meter_id, month_str)
    if not codes:
        return ([], 0.0, [], 0.0)

    ws_lines, ws_total = [], 0.0
    sd_lines, sd_total = [], 0.0

    for code in codes:
        tariff = _get_tariff_by_code(code, month_str)
        if not tariff:
            continue
        util = (tariff["utility_type"] or "").lower()
        rate = float(tariff["rate"] or 0.0)

        # Determine amount
        if code in ("WSSurcharge", "SDSurcharge"):
            amt = round((cons_kL or 0.0) * rate, 2)
            cons_show = round(cons_kL or 0.0, 2)
        else:
            amt = round(rate, 2)
            cons_show = 0

        line = {
            "label": (tariff.get("description") or code),
            "cons": cons_show,
            "rate": rate,
            "amount": amt,
        }

        if util == "water":
            ws_lines.append(line); ws_total += amt
        elif util == "sanitation":
            sd_lines.append(line); sd_total += amt
        else:
            # If tariff row has a non-water/non-sanitation util, ignore on Page 2
            continue

    return (ws_lines, round(ws_total, 2), sd_lines, round(sd_total, 2))

def price_water_meter(meter_id: int, cons_kL: float, days: int, month_str: str):
    """
    Full water pricing for a meter:
      - WS tiers + mapped WS fixed lines
      - SD tiers + mapped SD fixed lines
    Returns a dict with breakdown & totals (for Page 2) and the 3 headline totals (for Page 1).
    """
    ws_tier, ws_tier_total, sd_tier, sd_tier_total = _price_ws_sd_tiers(cons_kL, days, month_str)
    ws_fixed, ws_fixed_total, sd_fixed, sd_fixed_total = _fixed_lines_from_map(meter_id, month_str, cons_kL)

    ws_total = round(ws_tier_total + ws_fixed_total, 2)
    sd_total = round(sd_tier_total + sd_fixed_total, 2)
    water_total = round(ws_total + sd_total, 2)

    return {
        "ws_tier": ws_tier,
        "ws_fixed": ws_fixed,
        "ws_total": ws_total,
        "sd_tier": sd_tier,
        "sd_fixed": sd_fixed,
        "sd_total": sd_total,
        "water_total": water_total,
    }

def build_metsoa_rows(tenant_id: int, month_str: str):
    """
    Produces the exact 5 values your route returns:
      elec_rows, elec_total, water_rows, water_total, due_to_metro
    NOTE: tenant_id is unused here (your pipeline already scopes consumption by tenant+month).
    """
    cons = fetch_consumption_for_month(month_str)

    elec_rate = get_electricity_rate_for_month(month_str)
    elec_rows, elec_total = [], 0.0
    water_rows, water_total = [], 0.0

    for r in cons:
        util = (r["utility_type"] or "").lower()
        if util == "electricity":
            due = round((r["consumption"] or 0) * (elec_rate or 0), 2) if elec_rate is not None else None
            elec_rows.append({
                "meter_label": r["meter_label"],
                "prev_date":   r["prev_date"],
                "prev_value":  r["prev_value"],
                "curr_date":   r["curr_date"],
                "curr_value":  r["curr_value"],
                "days":        r["days"],
                "consumption": int(r["consumption"] or 0),
                "rate":        elec_rate,
                "due":         due,
            })
            elec_total += (due or 0.0)
        elif util == "water":
            # Page 1 seed + 3 lines (WS, SD, Water total), Page 2 details come from same calc.
            priced = price_water_meter(r["meter_id"], float(r["consumption"] or 0.0), int(r["days"] or 0), month_str)
            water_rows.append({
                "meter_label": r["meter_label"],
                "prev_date":   r["prev_date"],
                "prev_value":  r["prev_value"],
                "curr_date":   r["curr_date"],
                "curr_value":  r["curr_value"],
                "days":        r["days"],
                "consumption": int(r["consumption"] or 0),
                "ws_amount":   priced["ws_total"],
                "sd_amount":   priced["sd_total"],
                "water_amount": priced["water_total"],
            })
            water_total += priced["water_total"]

    elec_total  = round(elec_total, 2)
    water_total = round(water_total, 2)
    due_to_metro = round(elec_total + water_total, 2)
    return elec_rows, elec_total, water_rows, water_total, due_to_metro

def build_metsoa_page2_groups(tenant_id: int, month_str: str):
    """
    For Page 2 rendering. One 'group' per water meter:
      {
        meter_label, prev_date, curr_date, days, consumption,
        ws_lines: [{label, cons, rate, amount}, ...], ws_total,
        sd_lines: [{label, cons, rate, amount}, ...], sd_total
      }
    """
    cons = fetch_consumption_for_month(month_str)
    groups = []
    for r in cons:
        if (r["utility_type"] or "").lower() != "water":
            continue
        priced = price_water_meter(r["meter_id"], float(r["consumption"] or 0.0), int(r["days"] or 0), month_str)

        # Merge tier + fixed for display
        ws_lines = priced["ws_tier"] + priced["ws_fixed"]
        sd_lines = priced["sd_tier"] + priced["sd_fixed"]

        groups.append({
            "meter_label": r["meter_label"],
            "prev_date":   r["prev_date"],
            "prev_value":  r["prev_value"],
            "curr_date":   r["curr_date"],
            "curr_value":  r["curr_value"],
            "days":        r["days"],
            "consumption": int(r["consumption"] or 0),
            "ws_lines":    ws_lines,
            "ws_total":    priced["ws_total"],
            "sd_lines":    sd_lines,
            "sd_total":    priced["sd_total"],
        })
    return groups
