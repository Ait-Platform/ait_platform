# ---------- One-pass builder (no imports here) ----------

from sqlalchemy import text
from app.extensions import db

ZERO_WS_SD = {
    "ws_amount": 0.0,
    "sd_amount": 0.0,
    "water_cost": 0.0,   # ws + sd
    "ws_tiers": [],
    "ws_fixed": [],
    "sd_tiers": [],
    "sd_fixed": [],
}

def _first_of_month(month_str):
    # month_str like 'YYYY-MM'
    return f"{month_str}-01"

def _tariffs_by_code(month_str):
    month_day = _first_of_month(month_str)
    rows = db.session.execute(text("""
        SELECT t1.*
        FROM bil_tariff t1
        JOIN (
            SELECT code, MAX(effective_date) AS max_eff
            FROM bil_tariff
            WHERE effective_date <= :md
            GROUP BY code
        ) t2 ON t2.code = t1.code AND t2.max_eff = t1.effective_date
    """), {"md": month_day}).mappings().all()
    out = {}
    for r in rows:
        out[r["code"]] = r
    return out

def _map_rows_for_meter(meter_id, month_str):
    month_day = _first_of_month(month_str)
    return db.session.execute(text("""
        SELECT charge_code, utility_type, effective_start, effective_end, is_enabled,
               COALESCE(tariff_code_override, '') AS tariff_code_override
        FROM bil_meter_charge_map
        WHERE meter_id = :mid
          AND is_enabled = 1
          AND (effective_start IS NULL OR effective_start = '' OR effective_start <= :md)
          AND (effective_end   IS NULL OR effective_end   = '' OR effective_end   >= :md)
        ORDER BY charge_code
    """), {"mid": meter_id, "md": month_day}).mappings().all()

def _split_kl_over_tiers(cons_kl, days, blocks):
    """
    blocks: list of (block_start_l_per_day, block_end_l_per_day, rate_per_kl, label)
    Returns: (tiers_list, total_amount)
    """
    days = max(1, int(days or 0))
    rem_l = float(cons_kl or 0) * 1000.0
    tiers = []
    total = 0.0

    for bstart, bend, rate, label in blocks:
        # expand daily limits to this reading period
        if bend == 0:
            cap = float("inf")
        else:
            cap = bend * days
        floor = bstart * days

        # capacity for THIS tier
        tier_cap = max(cap - floor, 0.0)

        take_l = min(rem_l, tier_cap)
        if take_l < 1e-9:
            tiers.append({"label": label, "cons_kl": 0.0, "rate": rate, "due": 0.0})
            continue

        cons_kl_tier = take_l / 1000.0
        amt = round(cons_kl_tier * rate, 2)
        total += amt
        tiers.append({"label": label, "cons_kl": round(cons_kl_tier, 3), "rate": rate, "due": amt})
        rem_l -= take_l

    return tiers, round(total, 2)

def _calc_ws_sd_for_meter(meter_id, month_str, cons_kl, days, tariffs, map_rows):
    """
    Use the map to decide which charges apply.
    Returns dict like ZERO_WS_SD (ws_amount, sd_amount, water_cost, and tier/fixed breakdowns)
    """
    if not map_rows:
        return dict(ZERO_WS_SD)

    # build quick helpers
    def rate_of(code_or_override):
        c = code_or_override
        t = tariffs.get(c)
        return (t["rate"] if t else None)

    def red_of(code_or_override):
        t = tariffs.get(code_or_override)
        # if column missing/NULL => None
        return (t.get("reduction_factor") if t else None) if hasattr(t, "get") else None

    # Figure which tier sets are included
    include_ws_tier = any((r["charge_code"] in ("WS_Tiered",) and r["utility_type"] == "water") for r in map_rows)
    include_sd_tier = any((r["charge_code"] in ("SD_Tiered",) and r["utility_type"] == "sanitation") for r in map_rows)

    # Default to include tiers if not explicitly controlled and it’s a water meter.
    # (If you want it strictly by map, remove these defaults.)
    # We'll detect water via presence of any water charge rows:
    is_waterish = any(r["utility_type"] == "water" for r in map_rows)
    if is_waterish and not include_ws_tier:
        include_ws_tier = True
    if is_waterish and not include_sd_tier:
        include_sd_tier = True

    ws_tiers, sd_tiers = [], []
    ws_total, sd_total = 0.0, 0.0

    # WS (water & sanitation supply side) tiers
    if include_ws_tier:
        ws_blocks = []
        for code, label in [
            ("Tier1_W&S", "Tier 1 (0–200 L/day)"),
            ("Tier2_W&S", "Tier 2 (201–833 L/day)"),
            ("Tier3_W&S", "Tier 3 (834–1000 L/day)"),
            ("Tier4_W&S", "Tier 4 (1001–1500 L/day)"),
        ]:
            t = tariffs.get(code)
            if not t:
                continue
            bstart = float(t["block_start"] or 0.0)
            bend   = float(t["block_end"] or 0.0)
            rate   = float(t["rate"])
            ws_blocks.append((bstart, bend, rate, label))

        ws_tiers, ws_total = _split_kl_over_tiers(cons_kl, days, ws_blocks)

    # SD (sewer disposal) tiers with reductions
    if include_sd_tier:
        sd_blocks = []
        for code, label in [
            ("Tier1_SD", "Tier 1 (0–200 L/day @ reduction)"),
            ("Tier2_SD", "Tier 2 (201–833 L/day @ reduction)"),
            ("Tier3_SD", "Tier 3 (834–1000 L/day @ reduction)"),
            ("Tier4_SD", "Tier 4 (1001–1500 L/day @ reduction)"),
        ]:
            t = tariffs.get(code)
            if not t:
                continue
            bstart = float(t["block_start"] or 0.0)
            bend   = float(t["block_end"] or 0.0)
            rate   = float(t["rate"])
            red    = float(t.get("reduction_factor") or 1.0)
            # Implement reduction by scaling the rate effectively
            rate_eff = rate
            # We'll write the *reduced consumption* in the display rows too:
            sd_blocks.append((bstart, bend, rate_eff, label, red))

        # convert sd_blocks to generic blocks and then post-apply reduction on each tier line
        generic_blocks = [(b[0], b[1], b[2], b[3]) for b in sd_blocks]
        sd_tiers_raw, sd_total_raw = _split_kl_over_tiers(cons_kl, days, generic_blocks)

        sd_tiers = []
        sd_total = 0.0
        for raw, meta in zip(sd_tiers_raw, sd_blocks):
            red = meta[4]
            reduced_kl = round((raw["cons_kl"] or 0.0) * red, 3)
            amt = round(reduced_kl * (raw["rate"] or 0.0), 2)
            sd_tiers.append({
                "label":   raw["label"],
                "cons_kl": reduced_kl,
                "rate":    raw["rate"],
                "due":     amt,
            })
            sd_total += amt

    # Fixed/surcharge items via map (per-kL or per-month)
    ws_fixed, sd_fixed = [], []
    perkl_codes = {"WSSurcharge", "SDSurcharge"}  # multiply by cons_kl
    fixed_codes = {"WaterLossLevy", "RefuseBin", "MgmtFee"}  # monthly amount

    # Iterate the map rows to add fixed lines that apply
    for mr in map_rows:
        code = mr["tariff_code_override"] or mr["charge_code"]
        rate = rate_of(code)
        if rate is None:
            continue

        if mr["utility_type"] == "water":
            if code in perkl_codes:
                amt = round(rate * float(cons_kl or 0.0), 2)
                ws_fixed.append({"label": code, "cons_kl": float(cons_kl or 0.0), "rate": rate, "due": amt})
                ws_total += amt
            elif code in fixed_codes:
                amt = round(rate, 2)
                ws_fixed.append({"label": code, "cons_kl": None, "rate": rate, "due": amt})
                ws_total += amt

        elif mr["utility_type"] == "sanitation":
            if code in perkl_codes:
                amt = round(rate * float(cons_kl or 0.0), 2)
                sd_fixed.append({"label": code, "cons_kl": float(cons_kl or 0.0), "rate": rate, "due": amt})
                sd_total += amt
            elif code in fixed_codes:
                amt = round(rate, 2)
                sd_fixed.append({"label": code, "cons_kl": None, "rate": rate, "due": amt})
                sd_total += amt

    return {
        "ws_amount": round(ws_total, 2),
        "sd_amount": round(sd_total, 2),
        "water_cost": round(ws_total + sd_total, 2),
        "ws_tiers": ws_tiers,
        "ws_fixed": ws_fixed,
        "sd_tiers": sd_tiers,
        "sd_fixed": sd_fixed,
    }

def _get_consumption_rows(tenant_id, month_str):
    return db.session.execute(text("""
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
      JOIN bil_sectional_unit su ON su.id = m.sectional_unit_id
      JOIN bil_tenant t ON t.sectional_unit_id = su.id AND t.id = :tid
      WHERE c.month = :m
      ORDER BY CASE WHEN LOWER(m.utility_type) LIKE 'elec%%' THEN 0 ELSE 1 END,
               m.meter_number
    """), {"tid": tenant_id, "m": month_str}).mappings().all()

def _get_electric_rate(tariffs):
    t = tariffs.get("ElecRate")
    return float(t["rate"]) if t else None

def build_metsoa_payload(tenant_id, month_str):
    """
    Single pass over consumption rows:
      - Build Page 1 rows (electricity with due; water with WS/SD/WTR rows)
      - Build Page 2 water detail sections
    Returns: (page1_dict, page2_dict)
    """
    tariffs = _tariffs_by_code(month_str)
    base = _get_consumption_rows(tenant_id, month_str)
    map_cache = {}  # meter_id -> map rows

    elec_rows, elec_total = [], 0.0
    water_rows, water_total = [], 0.0
    sections = []   # page 2

    e_rate = _get_electric_rate(tariffs)

    for r in base:
        util = (r["utility_type"] or "").strip().lower()
        cons = int(r["consumption"] or 0)
        days = int(r["days"] or 0)

        if util.startswith("elec"):
            due = round((e_rate or 0.0) * cons, 2)
            elec_rows.append({
                "meter":       r["meter_label"],
                "prev_date":   r["prev_date"],
                "prev_value":  r["prev_value"],
                "curr_date":   r["curr_date"],
                "curr_value":  r["curr_value"],
                "days":        days,
                "consumption": cons,
                "rate":        e_rate,
                "due":         due,
            })
            elec_total += due
            continue

        # Water base line on Page 1
        water_rows.append({
            "kind":        "water-cons",
            "meter":       r["meter_label"],
            "prev_date":   r["prev_date"],
            "prev_value":  r["prev_value"],
            "curr_date":   r["curr_date"],
            "curr_value":  r["curr_value"],
            "days":        days,
            "consumption": cons,
            "rate":        None,
            "due":         None,
        })

        # Map rows (cache per meter)
        mr = map_cache.get(r["meter_id"])
        if mr is None:
            mr = _map_rows_for_meter(r["meter_id"], month_str)
            map_cache[r["meter_id"]] = mr

        totals = _calc_ws_sd_for_meter(
            meter_id=r["meter_id"],
            month_str=month_str,
            cons_kl=cons,         # consumption is stored as whole numbers of kL
            days=days,
            tariffs=tariffs,
            map_rows=mr
        ) or dict(ZERO_WS_SD)

        ws_amt = totals.get("ws_amount", 0.0)
        sd_amt = totals.get("sd_amount", 0.0)
        w_amt  = totals.get("water_cost", 0.0)

        # three summary lines on Page 1
        water_rows.append({"kind": "ws",          "label": f"W&S Cost for # {r['meter_label']}",  "rate": ws_amt})
        water_rows.append({"kind": "sd",          "label": f"S & D Cost # {r['meter_label']}",    "rate": sd_amt})
        water_rows.append({"kind": "water-total", "label": f"Water Cost for # {r['meter_label']}", "due":  w_amt})

        water_total += w_amt

        # Page 2 section for this meter
        sections.append({
            "meter":     r["meter_label"],
            "cons_kl":   cons,
            "days":      days,
            "ws_tiers":  totals.get("ws_tiers", []),
            "ws_fixed":  totals.get("ws_fixed", []),
            "ws_total":  totals.get("ws_amount", 0.0),
            "sd_tiers":  totals.get("sd_tiers", []),
            "sd_fixed":  totals.get("sd_fixed", []),
            "sd_total":  totals.get("sd_amount", 0.0),
            "grand":     totals.get("water_cost", 0.0),
        })

    due_to_metro = round(elec_total + water_total, 2)

    page1 = {
        "elec_rows": elec_rows,
        "elec_total": round(elec_total, 2),
        "water_rows": water_rows,
        "water_total": round(water_total, 2),
        "due_to_metro": due_to_metro,
    }
    page2 = {
        "sections": sections,
        "month": month_str,
    }
    return page1, page2

# ── tariffs & map lookups ──────────────────────────────────────────────────────
from datetime import date
from sqlalchemy import text

def _month_first(month_str: str) -> str:
    # '2025-06' -> '2025-06-01'
    return f"{month_str}-01"

def get_tariffs_by_prefix(prefix: str) -> list[dict]:
    """
    Returns tiered tariffs ordered by block_start, e.g.
    prefix='Tier' with utility filters done by caller.
    Each row has: code, rate, block_start, block_end, reduction_factor
    """
    rows = db.session.execute(text("""
        SELECT code, rate, block_start, block_end, COALESCE(reduction_factor, 1.0) AS reduction_factor
        FROM bil_tariff
        WHERE code LIKE :pfx || '%'
        ORDER BY block_start ASC
    """), {"pfx": prefix}).mappings().all()
    return rows

def get_tariffs_for_ws() -> list[dict]:
    # Tier1_W&S ... Tier4_W&S
    return db.session.execute(text("""
        SELECT code, rate, block_start, block_end
        FROM bil_tariff
        WHERE utility_type='water' AND code LIKE 'Tier%_W&S'
        ORDER BY block_start ASC
    """)).mappings().all()

def get_tariffs_for_sd() -> list[dict]:
    # Tier1_SD ... Tier4_SD (with reduction_factor)
    return db.session.execute(text("""
        SELECT code, rate, block_start, block_end, COALESCE(reduction_factor,1.0) AS reduction_factor
        FROM bil_tariff
        WHERE utility_type='sanitation' AND code LIKE 'Tier%_SD'
        ORDER BY block_start ASC
    """)).mappings().all()

def get_meter_map_for_month(meter_id: int, month_str: str) -> list[dict]:
    """
    All enabled map rows for this meter that are effective for month_str.
    """
    m1 = _month_first(month_str)
    rows = db.session.execute(text("""
        SELECT meter_id, charge_code, utility_type,
               COALESCE(effective_start,'1900-01-01') AS effective_start,
               effective_end, COALESCE(is_enabled,1) AS is_enabled,
               COALESCE(tariff_code_override,'') AS tariff_code_override
        FROM bil_meter_charge_map
        WHERE meter_id=:mid
          AND COALESCE(is_enabled,1)=1
          AND date(:m1) >= date(COALESCE(effective_start,'1900-01-01'))
          AND (effective_end IS NULL OR date(:m1) <= date(effective_end))
        ORDER BY meter_id, charge_code
    """), {"mid": meter_id, "m1": m1}).mappings().all()
    return rows

# ── tier math ─────────────────────────────────────────────────────────────────
def _split_by_blocks(cons_kl: int, days: int, blocks: list[dict]) -> list[dict]:
    """
    Johannesburg-style blocks are specified in L/DAY ranges (0–200, 201–833, ...).
    We prorate the allowance by 'days', then allocate 'cons_kl' across those caps.

    Returns list[ {code, cons_kl, rate, reduction_factor(optional), due} ] 
    with due not calculated here (caller applies rate×cons×reduction as needed).
    """
    # Convert L/day cap to kL for the period
    out = []
    remaining = float(cons_kl)

    for b in blocks:
        # Convert the L/day band to kL for 'days'
        lo = float(b["block_start"])
        hi = float(b["block_end"])
        band_kl = max(0.0, (hi - lo) * days / 1000.0)

        take = min(remaining, band_kl) if band_kl > 0 else 0.0
        out.append({
            "code": b["code"],
            "cons_kl": round(take, 2),
            "rate": float(b["rate"]),
            "reduction_factor": float(b.get("reduction_factor", 1.0)),
        })
        remaining -= take
        if remaining <= 1e-6:
            break

    # If there’s still consumption beyond last band, treat it as final band rate
    if remaining > 1e-6 and blocks:
        last = blocks[-1]
        out.append({
            "code": last["code"],
            "cons_kl": round(remaining, 2),
            "rate": float(last["rate"]),
            "reduction_factor": float(last.get("reduction_factor", 1.0)),
        })

    return out

# ── master calculator used by Page 1 + Page 2 ─────────────────────────────────
ZERO_WS_SD_TOTALS = {
    "ws_amount": 0.0, "sd_amount": 0.0, "water_cost": 0.0,
    "ws_breakdown": [], "sd_breakdown": [],
    "fixed_left": [], "fixed_right": []
}

def calc_ws_sd_totals(
    meter_id: int,
    month_str: str,
    consumption_kl: int,
    days: int,
    want_breakdown: bool = True,
) -> dict:
    """
    Uses the meter map to decide which charges apply.
    - WS_Tiered / SD_Tiered: compute by blocks
    - WSSurcharge / SDSurcharge: cons × rate
    - WaterLossLevy, MgmtFee, RefuseBin: flat
    Returns dict with totals + breakdown lists for Page 2.
    """
    try:
        cons = max(0, int(consumption_kl or 0))
        days = max(1, int(days or 0))
    except Exception:
        cons = max(0, int(consumption_kl or 0))
        days = 26

    mm = get_meter_map_for_month(meter_id, month_str)

    # Flags (fall back to auto-tier if a meter is water but map forgot tiers)
    has_ws_tier = any(r["charge_code"] in ("WS_Tiered", "WS_TIERED") for r in mm)
    has_sd_tier = any(r["charge_code"] in ("SD_Tiered", "SD_TIERED") for r in mm)

    ws_total = 0.0
    sd_total = 0.0
    ws_lines = []
    sd_lines = []
    left_fixed  = []  # show under WS column on Page 2
    right_fixed = []  # show under SD column on Page 2

    # ── Tiers: WS
    if has_ws_tier:
        ws_blocks = get_tariffs_for_ws()  # Tier1_W&S..Tier4_W&S
        ws_alloc = _split_by_blocks(cons, days, ws_blocks)
        for row in ws_alloc:
            due = round(row["cons_kl"] * row["rate"], 2)
            ws_total += due
            if want_breakdown and row["cons_kl"] > 0:
                ws_lines.append({
                    "label": row["code"].replace("_", " "),
                    "cons": row["cons_kl"], "rate": row["rate"], "due": due
                })

    # ── Tiers: SD (with reduction)
    if has_sd_tier:
        sd_blocks = get_tariffs_for_sd()  # Tier1_SD..Tier4_SD
        sd_alloc = _split_by_blocks(cons, days, sd_blocks)
        for row in sd_alloc:
            eff_cons = round(row["cons_kl"] * row["reduction_factor"], 2)
            due = round(eff_cons * row["rate"], 2)
            sd_total += due
            if want_breakdown and row["cons_kl"] > 0:
                sd_lines.append({
                    "label": row["code"].replace("_", " "),
                    "cons": eff_cons, "rate": row["rate"], "due": due
                })

    # ── Fixed charges from the map
    def _tariff_rate(code: str) -> float:
        r = db.session.execute(text("""
            SELECT rate FROM bil_tariff WHERE code=:c LIMIT 1
        """), {"c": code}).scalar()
        return float(r or 0.0)

    have_ws_surcharge = any(r["charge_code"] == "WSSurcharge" for r in mm)
    have_sd_surcharge = any(r["charge_code"] == "SDSurcharge" for r in mm)
    have_wll          = any(r["charge_code"] == "WaterLossLevy" for r in mm)
    have_mgmt         = any(r["charge_code"] == "MgmtFee" for r in mm)
    have_refuse       = any(r["charge_code"] == "RefuseBin" for r in mm)

    if have_ws_surcharge:
        rate = _tariff_rate("WSSurcharge")
        due  = round(cons * rate, 2)  # 0 consumption => 0 due
        ws_total += due
        left_fixed.append({"label": "Surcharge", "cons": cons, "rate": rate, "due": due})

    if have_wll:
        rate = _tariff_rate("WaterLossLevy")
        due  = round(rate, 2)
        ws_total += due
        left_fixed.append({"label": "Water Loss Levy", "cons": 0, "rate": rate, "due": due})

    if have_mgmt:
        rate = _tariff_rate("MgmtFee")
        due  = round(rate, 2)
        ws_total += due
        left_fixed.append({"label": "Monthly Management Fee", "cons": 0, "rate": rate, "due": due})

    if have_sd_surcharge:
        rate = _tariff_rate("SDSurcharge")
        # Apply surcharge to reduced volume? Your business rule said: *percentage reduction applies only to SD tiers*.
        # Surcharge = cons × rate (no reduction):
        due  = round(cons * rate, 2)
        sd_total += due
        right_fixed.append({"label": "Surcharge", "cons": cons, "rate": rate, "due": due})

    if have_refuse:
        rate = _tariff_rate("RefuseBin")
        due  = round(rate, 2)
        sd_total += due
        right_fixed.append({"label": "Refuse Bins", "cons": 0, "rate": rate, "due": due})

    water_cost = round(ws_total + sd_total, 2)

    return {
        "ws_amount": round(ws_total, 2),
        "sd_amount": round(sd_total, 2),
        "water_cost": water_cost,
        "ws_breakdown": ws_lines,
        "sd_breakdown": sd_lines,
        "fixed_left": left_fixed,
        "fixed_right": right_fixed,
    }
