# utils/billing_map.py

# Month helpers
from sqlalchemy import text
from app.extensions import db

def _month_bounds(month_str):
    # month_str: 'YYYY-MM'
    y = int(month_str[:4]); m = int(month_str[5:7])
    start = f"{y:04d}-{m:02d}-01"
    if m == 12:
        end = f"{y+1:04d}-01-01"
    else:
        end = f"{y:04d}-{m+1:02d}-01"
    return start, end

# Tariff lookup by code & month (latest <= month)
def _tariff_for_code_month(code, month_str):
    start, end = _month_bounds(month_str)
    row = db.session.execute(text("""
        SELECT id, utility_type, code, description, rate, block_start, block_end,
               effective_date, COALESCE(reduction_factor,1.0) AS reduction_factor,
               COALESCE(unit,'') AS unit
        FROM bil_tariff
        WHERE code = :code
          AND date(effective_date) < :end
        ORDER BY date(effective_date) DESC
        LIMIT 1
    """), {"code": code, "end": end}).mappings().first()
    return row

# Pull SD reduction from your table (preferred), else from bil_tariff.reduction_factor, else 1.0
def _sd_reduction_for_code(code, month_str):
    start, end = _month_bounds(month_str)
    # Try your reduction map first
    try:
        r = db.session.execute(text("""
            SELECT reduction_factor
            FROM SD_REDUCTION_BY_CODE
            WHERE code = :code
              AND date(effective_date) < :end
            ORDER BY date(effective_date) DESC
            LIMIT 1
        """), {"code": code, "end": end}).first()
        if r and r[0] is not None:
            return float(r[0])
    except Exception:
        pass
    # Fallback to bil_tariff
    t = _tariff_for_code_month(code, month_str)
    return float(t["reduction_factor"]) if t else 1.0

# Which charges apply to a meter this month (from your map)
def meter_mapped_charges(meter_id, month_str):
    start, end = _month_bounds(month_str)
    rows = db.session.execute(text("""
        SELECT mm.charge_code, mm.utility_type, mm.bill_to, mm.show_on_tenant,
               mm.effective_start, mm.effective_end, mm.is_enabled
        FROM bil_meter_charge_map mm
        WHERE mm.meter_id = :mid
          AND COALESCE(mm.is_enabled,1) = 1
          AND (mm.effective_start IS NULL OR date(mm.effective_start) < :end)
          AND (mm.effective_end   IS NULL OR date(mm.effective_end)   >= :start)
        ORDER BY mm.charge_code
    """), {"mid": meter_id, "start": start, "end": end}).mappings().all()
    return list(rows)

# Split kL into W&S tiers using bil_tariff rows (Tier1_W&S..Tier4_W&S)
def _ws_tiers(cons_kl, month_str):
    codes = ["Tier1_W&S","Tier2_W&S","Tier3_W&S","Tier4_W&S"]
    tiers = []
    remaining = float(cons_kl)
    lower = 0.0
    for code in codes:
        t = _tariff_for_code_month(code, month_str)
        if not t: 
            continue
        hi = float(t["block_end"]) if float(t["block_end"]) > 0 else float("inf")
        span = max(0.0, min(remaining, hi - lower))
        if span > 0:
            tiers.append({
                "code": code,
                "rate": float(t["rate"]),
                "unit": t["unit"],
                "from": lower, "to": hi if hi != float("inf") else None,
                "cons": span
            })
            remaining -= span
        lower = hi
        if remaining <= 1e-9:
            break
    return tiers

# SD tiers = same blocks, but each tier gets a reduction factor (from your table)
def _sd_tiers(cons_kl, month_str):
    ws = _ws_tiers(cons_kl, month_str)
    sd = []
    for tier in ws:
        rf = _sd_reduction_for_code(tier["code"].replace("_W&S","_SD"), month_str)
        sd.append({
            **tier,
            "code": tier["code"].replace("_W&S","_SD"),
            "cons_reduced": tier["cons"] * rf,
            "rf": rf,
            "rate": float((_tariff_for_code_month(tier["code"].replace("_W&S","_SD"), month_str) or {"rate":0})["rate"])
        })
    return sd

# Price W&S + SD tiers, and apply mapped extras (surcharges, levies, refuse, mgmt)
def compute_water_costs_for_meter(meter_id, cons_kl, month_str):
    cons_kl = float(cons_kl or 0)
    ws_t = _ws_tiers(cons_kl, month_str)
    sd_t = _sd_tiers(cons_kl, month_str)

    ws_amount = sum(t["cons"] * float(t["rate"]) for t in ws_t)
    sd_amount = sum(t["cons_reduced"] * float(t["rate"]) for t in sd_t)

    # Extras driven by meter map
    extras = []
    mapped = meter_mapped_charges(meter_id, month_str)
    # total volumes for surcharges
    total_ws_kl = sum(t["cons"] for t in ws_t)
    total_sd_kl = sum(t["cons_reduced"] for t in sd_t)

    for m in mapped:
        code = m["charge_code"]
        t = _tariff_for_code_month(code, month_str)
        if not t:
            continue
        unit = (t["unit"] or "").lower()
        rate = float(t["rate"])

        # Variable per kL
        if unit == "kl":
            if code.lower().startswith("ws"):
                qty = total_ws_kl
            elif code.lower().startswith("sd"):
                qty = total_sd_kl
            else:
                qty = total_ws_kl  # default to raw water kL
            amount = qty * rate
            extras.append({"code": code, "qty": qty, "rate": rate, "amount": amount, "unit": unit})
        else:
            # Fixed monthly ZAR/month (or anything not 'kL')
            amount = rate
            extras.append({"code": code, "qty": None, "rate": rate, "amount": amount, "unit": unit})

    total = ws_amount + sd_amount + sum(x["amount"] for x in extras)

    return {
        "ws_tiers": ws_t,
        "sd_tiers": sd_t,
        "ws_amount": ws_amount,
        "sd_amount": sd_amount,
        "extras": extras,
        "water_amount": ws_amount,   # keep explicit for Page 1
        "total_due": total
    }

# Electricity flat rate (map still screens extras if you ever add any)
def compute_electricity_due(cons_kwh, month_str):
    t = _tariff_for_code_month("ElecRate", month_str)
    rate = float(t["rate"]) if t else 0.0
    return {"rate": rate, "due": float(cons_kwh or 0) * rate}

# utils/billing_map.py

def build_metsoa_rows(tenant_id, month_str):
    """
    Returns:
      elec_rows: [ {meter_label, prev_date, prev_value, curr_date, curr_value, days, consumption, rate, due}, ... ]
      elec_total: float
      water_rows: [ {meter_label, prev_date, prev_value, curr_date, curr_value, days, consumption,
                     ws_amount, sd_amount, water_amount, extras:[{code, label, unit, rate, cons, amount}]}, ... ]
      water_total: float  (sum(ws_amount + sd_amount + water_amount + extras) across water meters)
      due_to_metro: float (elec_total + water_total)
    """
    # ---- helpers (kept local to avoid changing your imports) -----------------
    def _month_end(s):  # 'YYYY-MM' -> 'YYYY-MM-31' (safe for <= comparisons on 'YYYY-MM-DD')
        return f"{s}-31"

    def _tariff_by_code(code, utility_type, month_end):
        row = db.session.execute(text("""
            SELECT code, utility_type, description, rate, block_start, block_end, effective_date,
                   COALESCE(reduction_factor, 1.0) AS reduction_factor,
                   COALESCE(unit, '') AS unit
            FROM bil_tariff
            WHERE code = :code
              AND utility_type = :ut
              AND effective_date <= :me
            ORDER BY effective_date DESC
            LIMIT 1
        """), {"code": code, "ut": utility_type, "me": month_end}).mappings().first()
        return row

    def _elec_rate(month_end):
        r = db.session.execute(text("""
            SELECT rate
            FROM bil_tariff
            WHERE utility_type='electricity' AND code='ElecRate'
              AND effective_date <= :me
            ORDER BY effective_date DESC
            LIMIT 1
        """), {"me": month_end}).scalar()
        return float(r or 0)

    def _water_tiers(utility_type, month_end):
        # Pull all Tier* rows for the utility (water or sanitation), ordered by block_end (per-day liters)
        tiers = db.session.execute(text("""
            SELECT code, rate, block_start, block_end,
                   COALESCE(reduction_factor,1.0) AS reduction_factor
            FROM bil_tariff
            WHERE utility_type = :ut
              AND code LIKE 'Tier%_%'
              AND effective_date <= :me
            ORDER BY block_end ASC
        """), {"ut": utility_type, "me": month_end}).mappings().all()
        return tiers

    def _alloc_tiers(cons_kl, days, tiers, apply_reduction=False):
        """Allocate a monthly consumption (kL) across per-day tier bands.
           tier band capacity for period = (block_end - prev_block_end) liters/day * days / 1000
        """
        remaining = float(cons_kl or 0)
        prev_end = 0.0
        splits = []
        for t in tiers:
            band_lpd = max(0.0, float(t["block_end"] or 0) - float(prev_end))
            cap_kl = (band_lpd * float(days or 0)) / 1000.0
            take = min(remaining, cap_kl)
            rem_before = remaining
            remaining -= take
            amt = float(t["rate"] or 0)
            red = float(t["reduction_factor"] or 1.0) if apply_reduction else 1.0
            # SD uses reduced volume for billing
            bill_kl = take * red
            due = bill_kl * amt
            splits.append({
                "code": t["code"],
                "band_cap_kl": cap_kl,
                "take_kl": take,
                "bill_kl": bill_kl,
                "rate": amt,
                "reduction": red,
                "due": due,
            })
            prev_end = float(t["block_end"] or prev_end)
            if remaining <= 1e-9:
                break
        # If consumption exceeds last tier cap, bill the overflow at last tier rate (and reduction if SD)
        if remaining > 1e-9 and tiers:
            t = tiers[-1]
            amt = float(t["rate"] or 0)
            red = float(t["reduction_factor"] or 1.0) if apply_reduction else 1.0
            bill_kl = remaining * red
            due = bill_kl * amt
            splits.append({
                "code": t["code"] + "_OVR",
                "band_cap_kl": 0.0,
                "take_kl": remaining,
                "bill_kl": bill_kl,
                "rate": amt,
                "reduction": red,
                "due": due,
            })
            remaining = 0.0
        total_due = sum(x["due"] for x in splits)
        return splits, total_due

    def _mapped_extras(meter_id, cons_kl, month_end):
        """Returns list of extra charges mapped to this meter for the month."""
        # All active mappings for month
        mm = db.session.execute(text("""
            SELECT charge_code, utility_type, show_on_tenant
            FROM bil_meter_charge_map
            WHERE meter_id = :mid
              AND is_enabled = 1
              AND effective_start <= :me
              AND (effective_end IS NULL OR effective_end >= :me)
        """), {"mid": meter_id, "me": month_end}).mappings().all()

        extras = []
        for mrow in mm:
            t = db.session.execute(text("""
                SELECT code, description, rate, COALESCE(unit,'') AS unit
                FROM bil_tariff
                WHERE code = :code
                  AND utility_type = :ut
                  AND effective_date <= :me
                ORDER BY effective_date DESC
                LIMIT 1
            """), {"code": mrow["charge_code"], "ut": mrow["utility_type"], "me": month_end}).mappings().first()
            if not t:
                continue
            unit = (t["unit"] or "").lower()
            # safety net: surcharges are per kL
            if not unit and t["code"] in ("WSSurcharge", "SDSurcharge"):
                unit = "kL"

            rate = float(t["rate"] or 0)
            if unit in ("kl", "per_kl", "zar/kl"):
                amt = rate * float(cons_kl or 0)
                cons_used = float(cons_kl or 0)
            else:
                # Treat anything else as a fixed monthly amount
                amt = rate
                cons_used = 0.0
            extras.append({
                "code": t["code"],
                "label": t["description"] or t["code"],
                "unit": unit or "",
                "rate": rate,
                "cons": cons_used,
                "amount": amt,
            })
        return extras

    # ---- load tenant’s consumption (no c.tenant_id in table; join via sectional_unit) ----
    rows = db.session.execute(text("""
        SELECT
          c.meter_id,
          m.meter_number     AS meter_label,
          lower(m.utility_type) AS utility_type,
          c.last_date        AS prev_date,
          c.last_read        AS prev_value,
          c.new_date         AS curr_date,
          c.new_read         AS curr_value,
          c.days             AS days,
          c.consumption      AS consumption
        FROM bil_consumption c
        JOIN bil_meter m           ON m.id = c.meter_id
        JOIN bil_sectional_unit su ON su.id = m.sectional_unit_id
        JOIN bil_tenant t          ON t.sectional_unit_id = su.id
        WHERE t.id = :tid
          AND c.month = :month
        ORDER BY CASE WHEN lower(m.utility_type)='electricity' THEN 0 ELSE 1 END,
                 m.meter_number
    """), {"tid": tenant_id, "month": month_str}).mappings().all()

    me = _month_end(month_str)

    # ---- ELECTRICITY --------------------------------------------------------
    elec_rate = _elec_rate(me)
    elec_rows = []
    elec_total = 0.0

    for r in rows:
        if (r["utility_type"] or "") != "electricity":
            continue
        cons = int(r["consumption"] or 0)
        due = float(cons) * float(elec_rate)
        elec_total += due
        elec_rows.append({
            "meter_label": r["meter_label"],
            "prev_date": r["prev_date"],
            "prev_value": r["prev_value"],
            "curr_date": r["curr_date"],
            "curr_value": r["curr_value"],
            "days": r["days"],
            "consumption": cons,
            "rate": elec_rate,
            "due": due,
        })

    # ---- WATER (WS + SD tiers, plus water potable + mapped extras) ----------
    water_rows = []
    water_total = 0.0

    wtiers = _water_tiers("water", me)       # Tier1_W&S … Tier4_W&S
    sdtier = _water_tiers("sanitation", me)  # Tier1_SD  … Tier4_SD

    for r in rows:
        if (r["utility_type"] or "") != "water":
            continue
        cons_kl = float(int(r["consumption"] or 0))  # ensure whole number
        days = int(r["days"] or 0)

        ws_splits, ws_due = _alloc_tiers(cons_kl, days, wtiers, apply_reduction=False)
        sd_splits, sd_due = _alloc_tiers(cons_kl, days, sdtier, apply_reduction=True)

        # potable water cost: by policy you provided, treat as same tiers as WS (if distinct, swap to its table)
        water_amount = ws_due

        extras = _mapped_extras(r["meter_id"], cons_kl, me)
        extras_total = sum(x["amount"] for x in extras)

        total_this_meter = ws_due + sd_due + water_amount + extras_total
        water_total += total_this_meter

        water_rows.append({
            "meter_label": r["meter_label"],
            "prev_date": r["prev_date"],
            "prev_value": r["prev_value"],
            "curr_date": r["curr_date"],
            "curr_value": r["curr_value"],
            "days": r["days"],
            "consumption": int(cons_kl),
            # page-1 rendering: WS/SD appear stacked in the Rate column, Water in Due column
            "ws_amount": ws_due,
            "sd_amount": sd_due,
            "water_amount": water_amount,
            "extras": extras,   # if the template wants to show them beneath a meter
        })

    due_to_metro = elec_total + water_total
    return elec_rows, elec_total, water_rows, water_total, due_to_metro



