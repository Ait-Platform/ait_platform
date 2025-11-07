# app/billing/water.py
from __future__ import annotations
from decimal import Decimal, ROUND_HALF_UP, getcontext
from datetime import datetime, date
from sqlalchemy import text
from app.extensions import db

from calendar import monthrange

# Money-safe
getcontext().prec = 28
Q2 = Decimal("0.01")

def _D(x) -> Decimal:
    try:
        return Decimal(str(0 if x is None else x))
    except Exception:
        return Decimal("0")

def _q2(x: Decimal) -> Decimal:
    return x.quantize(Q2, rounding=ROUND_HALF_UP)

# ---------------------------
# Tariff codes you shared
# ---------------------------

WATER_TIER_CODES = [
    ("0L-200L/31Days",   "Tier1_W&S", 10, 200),   # liters/day
    ("201L-833L/31Days", "Tier2_W&S", 20, 833),
    ("833L-1KL/31Days",  "Tier3_W&S", 30, 1000),
    ("1KL-1,5KL/31Days", "Tier4_W&S", 40, 1500),
]

SAN_TIER_CODES = [
    ("0L-200L/31Days",   "Tier1_SD", 10, 200),
    ("201L-833L/31Days", "Tier2_SD", 20, 833),
    ("833L-1KL/31Days",  "Tier3_SD", 30, 1000),
    ("1KL-1,5KL/31Days", "Tier4_SD", 40, 1500),
]

def _side_from_utility(u: str | None) -> str:
    u = (u or "").strip().lower()
    if u in ("sanitation", "refuse"):
        return "SD"
    return "WS"

def _tariff_latest_by_code(code: str) -> dict:
    """Return newest tariff row for a code: {rate, rf, unit}. Missing -> zeros."""
    row = db.session.execute(
        text("""
            SELECT rate,
                   COALESCE(reduction_factor,1.0) AS rf,
                   COALESCE(unit,'') AS unit
            FROM bil_tariff
            WHERE code = :c
            ORDER BY date(effective_date) DESC
            LIMIT 1
        """),
        {"c": code},
    ).mappings().first()
    if not row:
        return {"rate": Decimal("0"), "rf": Decimal("1"), "unit": ""}
    return {"rate": _D(row["rate"]), "rf": _D(row["rf"]), "unit": (row["unit"] or "")}

def _is_per_kl(unit: str) -> bool:
    return "kl" in (unit or "").lower()

def _first_of_month(month_str: str) -> str:
    return f"{month_str}-01"  # month_str is 'YYYY-MM'

def _enabled_map_rows(meter_id: int, month_str: str) -> list[dict]:
    """Read your bil_meter_charge_map for this meter and month."""
    rows = db.session.execute(
        text("""
            SELECT meter_id, charge_code, utility_type,
                   effective_start, effective_end,
                   COALESCE(is_enabled,1) AS is_enabled,
                   COALESCE(tariff_code_override,'') AS tariff_code_override
            FROM bil_meter_charge_map
            WHERE meter_id = :m
              AND COALESCE(is_enabled,1) = 1
              AND (effective_start IS NULL OR date(effective_start) <= date(:d))
              AND (effective_end   IS NULL OR date(effective_end)   >= date(:d))
        """),
        {"m": meter_id, "d": _first_of_month(month_str)},
    ).mappings().all()
    return [dict(r) for r in rows]

def t_label(code: str) -> str:
    """Friendly label from bil_tariff.description fallback to code."""
    m = db.session.execute(
        text("""
            SELECT COALESCE(description, :c) AS d
            FROM bil_tariff
            WHERE code=:c
            ORDER BY date(effective_date) DESC
            LIMIT 1
        """),
        {"c": code},
    ).mappings().first()
    return (m["d"] if m else code)

# -------------------------------------------------
# PUBLIC: core calculator used by Page 2 and Page 1
# -------------------------------------------------
def calc_ws_sd_totals(
    *,
    meter_id: int,
    month_str: str,
    consumption_kl: float | int,
    days: int | None = None,
    include_fixed: bool = True,
    want_breakdown: bool = True,
) -> dict:
    """
    Compute water (WS) & sewer (SD) for one meter/month using your tariffs and map.
    - Tier caps are liters/day × days; T4 is remainder (no 'Above 1.5kL' row).
    - SD tiers use tariff reduction_factor as % reduction for quantity.
    - Fixed charges pulled from bil_meter_charge_map (side from utility_type).
      Per-kL if bil_tariff.unit contains 'kL', otherwise fixed.
    Returns: {ws_total, sd_total, water_cost, ws_lines[], sd_lines[]}
    """
    d_days = _D(days or 31)
    cons   = _D(consumption_kl)

    # caps in kL for the period
    def kl_cap(lpd: int) -> Decimal:
        return (_D(lpd) / _D(1000)) * d_days

    cap1 = kl_cap(200)
    cap2 = kl_cap(833)
    cap3 = kl_cap(1000)

    t1 = min(cons, cap1)
    t2 = min(max(cons - cap1, 0), max(cap2 - cap1, 0))
    t3 = min(max(cons - cap2, 0), max(cap3 - cap2, 0))
    t4 = max(cons - cap3, 0)

    bands_ws = [
        (WATER_TIER_CODES[0][0], t1, WATER_TIER_CODES[0][1], WATER_TIER_CODES[0][2]),
        (WATER_TIER_CODES[1][0], t2, WATER_TIER_CODES[1][1], WATER_TIER_CODES[1][2]),
        (WATER_TIER_CODES[2][0], t3, WATER_TIER_CODES[2][1], WATER_TIER_CODES[2][2]),
        (WATER_TIER_CODES[3][0], t4, WATER_TIER_CODES[3][1], WATER_TIER_CODES[3][2]),
    ]
    bands_sd_meta = [
        (SAN_TIER_CODES[0][0], SAN_TIER_CODES[0][1], SAN_TIER_CODES[0][2]),
        (SAN_TIER_CODES[1][0], SAN_TIER_CODES[1][1], SAN_TIER_CODES[1][2]),
        (SAN_TIER_CODES[2][0], SAN_TIER_CODES[2][1], SAN_TIER_CODES[2][2]),
        (SAN_TIER_CODES[3][0], SAN_TIER_CODES[3][1], SAN_TIER_CODES[3][2]),
    ]
    band_qtys = [t1, t2, t3, t4]

    def _add_line(lst, desc: str, qty: Decimal | None, rate: Decimal | None, due: Decimal, order: int):
        lst.append({
            "desc": desc,
            "qty":  (None if qty is None else qty.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)),
            "rate": (None if rate is None else _q2(rate)),
            "due":  _q2(due),
            "order": order,
        })

    ws_lines: list[dict] = []
    sd_lines: list[dict] = []

    # WS tiers
    for label, qty, code, order in bands_ws:
        if qty <= 0:
            continue
        t = _tariff_latest_by_code(code)
        due = qty * t["rate"] * t["rf"]
        _add_line(ws_lines, label, qty, t["rate"], due, order)

    # SD tiers (qty reduced via reduction_factor)
    sd_total_qty = Decimal("0")
    for (label, code, order), qty in zip(bands_sd_meta, band_qtys):
        if qty <= 0:
            continue
        t = _tariff_latest_by_code(code)
        red = t["rf"] if t["rf"] > 0 else Decimal("1")
        sd_qty = (qty * red).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        sd_total_qty += sd_qty
        due = sd_qty * t["rate"]   # rf applied to qty already
        _add_line(sd_lines, label, sd_qty, t["rate"], due, order)

    # Fixed charges from map
    if include_fixed:
        for r in _enabled_map_rows(meter_id, month_str):
            code = (r.get("tariff_code_override") or "").strip() or (r.get("charge_code") or "").strip()
            if not code:
                continue
            t = _tariff_latest_by_code(code)
            if t["rate"] <= 0:
                continue
            side = _side_from_utility(r.get("utility_type"))
            if _is_per_kl(t["unit"]):
                qty = cons if side == "WS" else sd_total_qty
                due = qty * t["rate"]
            else:
                qty = Decimal("1")
                due = t["rate"]
            label = t_label(code)
            if side == "SD":
                _add_line(sd_lines, label, qty, t["rate"], due, 900)
            else:
                _add_line(ws_lines, label, qty, t["rate"], due, 900)

    ws_total = _q2(sum((ln["due"] for ln in ws_lines), Decimal("0")))
    sd_total = _q2(sum((ln["due"] for ln in sd_lines), Decimal("0")))
    water_cost = _q2(ws_total + sd_total)

    ws_lines = sorted(ws_lines, key=lambda x: x["order"])
    sd_lines = sorted(sd_lines, key=lambda x: x["order"])

    return {
        "ws_total": ws_total,
        "sd_total": sd_total,
        "water_cost": water_cost,
        "ws_lines": ws_lines if want_breakdown else [],
        "sd_lines": sd_lines if want_breakdown else [],
    }


def upsert_meter_month_total(*, tenant_id: int, meter_id: int, month: str,
                             ws: float, sd: float, wc: float) -> None:
    month = datetime.strptime(month, "%Y-%m").strftime("%Y-%m")
    db.session.execute(text("""
        INSERT INTO bil_metsoa_meter_month
            (tenant_id, meter_id, month, utility_type,
             ws_total, sd_total, water_cost, total_due, updated_at)
        VALUES (:t, :m, :mon, 'water',
                :ws, :sd, :wc, (:ws + :sd), CURRENT_TIMESTAMP)
        ON CONFLICT (tenant_id, meter_id, month)
        DO UPDATE SET
            ws_total   = excluded.ws_total,
            sd_total   = excluded.sd_total,
            water_cost = excluded.water_cost,
            total_due  = (excluded.ws_total + excluded.sd_total),
            updated_at = CURRENT_TIMESTAMP
    """), {
        "t": tenant_id, "m": meter_id, "mon": month,
        "ws": round(ws, 2), "sd": round(sd, 2), "wc": round(wc, 2)
    })
    db.session.commit()

def upsert_tenant_month_water_totals(*, tenant_id: int, month: str, ws: float, sd: float, wc: float) -> None:
    month = datetime.strptime(month, "%Y-%m").strftime("%Y-%m")
    db.session.execute(text("""
        INSERT INTO bil_metsoa_tenant_month
            (tenant_id, month, ws_total, sd_total, water_total, updated_at)
        VALUES (:t, :mon, :ws, :sd, :wc, CURRENT_TIMESTAMP)
        ON CONFLICT (tenant_id, month)
        DO UPDATE SET
            ws_total    = excluded.ws_total,
            sd_total    = excluded.sd_total,
            water_total = excluded.water_total,
            updated_at  = CURRENT_TIMESTAMP
    """), {"t": tenant_id, "mon": month,
           "ws": round(ws, 2), "sd": round(sd, 2), "wc": round(wc, 2)})
    db.session.commit()

def recompute_and_upsert_water_totals(tenant_id: int, month: str) -> tuple[float, float, float]:
    """Recalculate for all water meters, upsert per-meter + aggregate; return (ws, sd, wc)."""
    month = datetime.strptime(month, "%Y-%m").strftime("%Y-%m")
    base_rows = get_consumption_rows_for_month(tenant_id, month) or []  # <-- your existing function
    water_bases = [r for r in base_rows if (r.get("utility_type") or "").strip().lower().startswith("w")]

    page_ws_total = 0.0
    page_sd_total = 0.0
    page_water_total = 0.0

    for r in water_bases:
        cons = int(r.get("consumption") or 0)
        days = int(r.get("days") or 0)
        totals = calc_ws_sd_totals(
            meter_id=r["meter_id"],
            month_str=month,
            consumption_kl=cons,
            days=days,
            include_fixed=True,
            want_breakdown=True,
        )
        upsert_meter_month_total(
            tenant_id=tenant_id,
            meter_id=r["meter_id"],
            month=month,
            ws=float(totals["ws_total"]),
            sd=float(totals["sd_total"]),
            wc=float(totals["water_cost"]),
        )
        page_ws_total   += float(totals["ws_total"])
        page_sd_total   += float(totals["sd_total"])
        page_water_total += float(totals["water_cost"])

    upsert_tenant_month_water_totals(
        tenant_id=tenant_id, month=month,
        ws=page_ws_total, sd=page_sd_total, wc=page_water_total,
    )
    return round(page_ws_total, 2), round(page_sd_total, 2), round(page_water_total, 2)

# ------------- external dependency you already have -------------

def get_consumption_rows_for_month(tenant_id: int, month_str: str):
    """
    Return the base rows (electric + water) for the current METSOA build.
    We filter ONLY by month because the consumption table for this run
    already holds the rows we want.
    """
    rows = db.session.execute(text("""
        SELECT
          c.meter_id                                    AS meter_id,
          COALESCE(m.meter_number, CAST(m.id AS TEXT))  AS meter_label,
          LOWER(m.utility_type)                         AS utility_type,
          c.last_date                                   AS prev_date,
          c.last_read                                   AS prev_value,
          c.new_date                                    AS curr_date,
          c.new_read                                    AS curr_value,
          c.days                                        AS days,
          c.consumption                                 AS consumption
        FROM bil_consumption c
        JOIN bil_meter m ON m.id = c.meter_id
        WHERE c.month = :m
        ORDER BY CASE WHEN LOWER(m.utility_type) LIKE 'elec%' THEN 0 ELSE 1 END,
                 m.meter_number
    """), {"m": month_str}).mappings().all()
    return rows

def build_page2_sections(tenant_id: int, month_str: str):
    """
    Returns a list of sections, one per water meter:
      { header:{meter_label, prev_date, ...}, ws_lines:[...], sd_lines:[...],
        ws_total, sd_total }
    """
    base = get_consumption_rows_for_month(tenant_id, month_str)
    sections = []
    for r in base:
        if (r["utility_type"] or "").lower() != "water":
            continue
        ws, sd, ws_lines, sd_lines = calc_ws_sd_for_meter(
            r["meter_id"], float(r["consumption"] or 0), int(r["days"] or 0), month_str
        )
        sections.append({
            "header": r,
            "ws_lines": ws_lines,
            "sd_lines": sd_lines,
            "ws_total": _fmt_money(ws),
            "sd_total": _fmt_money(sd),
        })
    return sections

def calc_ws_sd_for_meter(meter_id: int, kL: float, days: int, month_str: str):
    """
    Returns:
      ws_total, sd_total,
      ws_lines: list of dicts {label, cons, rate, due}
      sd_lines: list of dicts {label, cons, rate, due}
    """
    ws_tiers, sd_tiers = get_water_tiers(month_str)
    fixed = get_fixed_tariffs(month_str)
    mapp  = get_fixed_map_for_meter(meter_id, month_str)

    # 1) Tiers
    ws_break = _apply_tiers(kL, ws_tiers)
    sd_break = _apply_tiers_sd(kL, sd_tiers)

    ws_lines = []
    ws_total = 0.0
    for vol, rate, cost in ws_break:
        ws_total += cost
        if vol > 0:
            ws_lines.append({"label": f"Tier @ {rate:.2f}", "cons": vol, "rate": rate, "due": cost})

    sd_lines = []
    sd_total = 0.0
    for vol, rate, red, cost in sd_break:
        sd_total += cost
        if vol > 0:
            sd_lines.append({"label": f"Tier (×{red:.2f}) @ {rate:.2f}", "cons": vol, "rate": rate, "due": cost})

    # 2) Fixed items from map
    # expected utility_type in map: 'water' or 'sanitation'
    # consumption-based surcharge: rate * kL (only if kL>0)
    def add_fixed(code, group, label, cons_mode):
        # group: 'water' or 'sanitation'
        if (code, group) not in mapp:
            return 0.0, None  # not enabled
        rate = fixed.get(code)
        if rate is None:
            return 0.0, None
        if cons_mode == "cons":
            cons = float(kL) if kL > 0 else 0.0
            due = cons * float(rate)
        else:
            cons = 0.0
            due = float(rate)
        line = {"label": label, "cons": cons, "rate": float(rate), "due": due}
        return due, line

    # Water-side
    add_due, line = add_fixed("WSSurcharge", "water", "Surcharge", "cons")
    if line: ws_lines.append(line); ws_total += add_due
    add_due, line = add_fixed("WaterLossLevy", "water", "Water Loss Levy", "flat")
    if line: ws_lines.append(line); ws_total += add_due
    add_due, line = add_fixed("MgmtFee", "water", "Monthly Management Fee", "flat")
    if line: ws_lines.append(line); ws_total += add_due

    # Sanitation-side
    add_due, line = add_fixed("SDSurcharge", "sanitation", "Surcharge", "cons")
    if line: sd_lines.append(line); sd_total += add_due
    add_due, line = add_fixed("RefuseBin", "sanitation", "Refuse Bins", "flat")
    if line: sd_lines.append(line); sd_total += add_due

    return _fmt_money(ws_total), _fmt_money(sd_total), ws_lines, sd_lines

def _fmt_money(x):
    return None if x is None else round(float(x), 2)

def get_water_tiers(month_str: str):
    """Return ordered tiers for WS and SD with reduction factors."""
    start, end = _month_bounds(month_str)
    ws = db.session.execute(text("""
        SELECT block_start, block_end, rate
        FROM bil_tariff
        WHERE utility_type='water'
          AND code LIKE 'Tier%_W&S'
          AND date(effective_date) <= :end
        ORDER BY block_start
    """), {"end": end}).mappings().all()
    sd = db.session.execute(text("""
        SELECT block_start, block_end, rate, COALESCE(reduction_factor,1.0) AS red
        FROM bil_tariff
        WHERE utility_type='sanitation'
          AND code LIKE 'Tier%_SD'
          AND date(effective_date) <= :end
        ORDER BY block_start
    """), {"end": end}).mappings().all()
    return [dict(r) for r in ws], [dict(r) for r in sd]

def get_fixed_map_for_meter(meter_id: int, month_str: str):
    """
    Meter charge map -> which fixed lines apply to this meter this month.
    Expected columns: (meter_id, charge_code, utility_type, is_enabled, effective_start, effective_end)
    If your table names/columns differ, adjust here only.
    """
    start, end = _month_bounds(month_str)
    sql = """
      SELECT charge_code, utility_type, COALESCE(is_enabled,1) AS is_enabled
      FROM bil_meter_charge_map
      WHERE meter_id = :mid
        AND COALESCE(is_enabled,1) = 1
        AND date(effective_start) <= :end
        AND (effective_end IS NULL OR date(effective_end) >= :start)
    """
    try:
        rows = db.session.execute(text(sql), {"mid": meter_id, "start": start, "end": end}).mappings().all()
        mapped = [(r["charge_code"], (r["utility_type"] or "").lower()) for r in rows]
    except Exception:
        mapped = []  # if table not present we fallback to nothing
    return set(mapped)


def get_fixed_tariffs(month_str: str):
    """Lookup fixed/surcharge tariff rates (code -> rate)."""
    start, end = _month_bounds(month_str)
    rows = db.session.execute(text("""
        SELECT code, rate
        FROM bil_tariff
        WHERE utility_type IN ('water','sanitation','refuse','management')
          AND date(effective_date) <= :end
    """), {"end": end}).mappings().all()
    ret = {r["code"]: float(r["rate"]) for r in rows}
    # Consider legacy codes you shared:
    # WaterLossLevy, WSSurcharge, SDSurcharge, RefuseBin, MgmtFee
    return ret

def _month_bounds(month_str):
    # month_str = "YYYY-MM"
    y, m = map(int, month_str.split("-"))

    first = date(y, m, 1)
    # next month
    if m == 12:
        nxt = date(y + 1, 1, 1)
    else:
        nxt = date(y, m + 1, 1)
    last = date(y, m, monthrange(y, m)[1])
    return first, last, nxt

def _apply_tiers(kL: float, tiers):
    """Return list of (vol, rate, cost) per tier for a consumption in kL."""
    rem = float(kL)
    out = []
    for t in tiers:
        lo = float(t["block_start"])
        hi = float(t["block_end"])
        span = max(0.0, hi - lo + 1.0) if hi and hi >= lo else max(0.0, rem)  # forgiving
        take = min(rem, span) if span > 0 else rem
        if take <= 0:
            out.append((0.0, float(t["rate"]), 0.0))
            continue
        cost = take * float(t["rate"])
        out.append((take, float(t["rate"]), cost))
        rem -= take
        if rem <= 0:
            break
    if rem > 0 and tiers:
        # spill remainder at last tier rate
        last_rate = float(tiers[-1]["rate"])
        out.append((rem, last_rate, rem * last_rate))
    return out

def _apply_tiers_sd(kL: float, tiers_sd):
    """Same as above, but include reduction_factor per tier."""
    rem = float(kL)
    out = []
    for t in tiers_sd:
        lo = float(t["block_start"])
        hi = float(t["block_end"])
        red = float(t.get("red", 1.0))
        span = max(0.0, hi - lo + 1.0) if hi and hi >= lo else max(0.0, rem)
        take = min(rem, span) if span > 0 else rem
        if take <= 0:
            out.append((0.0, float(t["rate"]), red, 0.0))
            continue
        eff_vol = take * red
        cost = eff_vol * float(t["rate"])
        out.append((take, float(t["rate"]), red, cost))
        rem -= take
        if rem <= 0:
            break
    if rem > 0 and tiers_sd:
        last_rate = float(tiers_sd[-1]["rate"])
        red = float(tiers_sd[-1].get("red", 1.0))
        eff_vol = rem * red
        out.append((rem, last_rate, red, eff_vol * last_rate))
    return out

def get_water_totals_from_db(tenant_id: int, month: str) -> tuple[float, float, float]:
    row = db.session.execute(text("""
        SELECT
          COALESCE(NULLIF(SUM(ws_total),0),  SUM(ws_amount))   AS ws,
          COALESCE(NULLIF(SUM(sd_total),0),  SUM(sd_amount))   AS sd,
          COALESCE(NULLIF(SUM(water_cost),0),SUM(water_amount)) AS water
        FROM bil_metsoa_meter_month
        WHERE tenant_id=:t AND month=:mon AND utility_type='water'
    """), {"t": tenant_id, "mon": month}).mappings().first()
    return (float(row["ws"] or 0), float(row["sd"] or 0), float(row["water"] or 0))
