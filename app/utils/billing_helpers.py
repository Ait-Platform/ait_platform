# app/utils/billing_helpers.py
# (No imports; all pure-Python helpers you can call from admin/billing)
import calendar 
from app.admin.billing.water import get_consumption_rows_for_month
from app.models.billing import (
    BilConsumption,  BilTenant, BilMeter,
    BilTariff )
from datetime import datetime, date, timedelta
from app.extensions import db
from sqlalchemy import func, and_, text

from decimal import Decimal, ROUND_HALF_UP

from calendar import monthrange

ZERO_WS_SD_TOTALS = {
    "ws_amount": 0.0,
    "sd_amount": 0.0,
    "water_amount": 0.0,  # ws + sd combined (what Page 1 shows as “Water Cost …”)
    "ws_rate": None,      # optional display rate
    "sd_rate": None,      # optional display rate
}


# Default daily cumulative caps (kL/day): 0–200L, 201–833L, 834–1000L, 1000–1500L+
CAPS_PER_DAY_DEFAULT = [0.200, 0.833, 1.000, 1.500]

def prorate_caps(days, caps_per_day=None):
    caps = (caps_per_day or CAPS_PER_DAY_DEFAULT)
    out = []
    for c in caps:
        out.append(c * (days / 30.0))
    return out  # cumulative caps for the given period (except the last is treated as unlimited)

def cumulative_allocate(cons_kl, days, caps_per_day=None):
    """
    Allocate consumption across tiers using cumulative caps.
    The last tier is treated as unlimited (anything remaining goes there).
    Returns [u1, u2, u3, u4].
    """
    caps = prorate_caps(days, caps_per_day)
    allocations = []
    rem = float(cons_kl)
    used_so_far = 0.0

    for i, cap in enumerate(caps):
        if i < len(caps) - 1:
            headroom = cap - used_so_far
            if headroom < 0:
                headroom = 0.0
            take = rem if rem <= headroom else headroom
            take = 0.0 if take < 0 else take
            allocations.append(take)
            rem -= take
            used_so_far += take
        else:
            # last tier: unlimited remainder
            take = rem if rem > 0 else 0.0
            allocations.append(take)
            rem -= take
            used_so_far += take

    # Ensure no tiny negative due to float noise
    return [0.0 if a < 1e-9 else a for a in allocations]

def tier_rows(labels, usages, rates):
    """
    Build tier rows [{'label','cons','rate','due'}] and total due for tiers.
    'labels' aligns with 'usages' and 'rates'.
    """
    rows = []
    total = 0.0
    for lbl, u, r in zip(labels, usages, rates):
        due = float(u) * float(r)
        rows.append({
            "label": lbl,
            "cons": float(u),
            "rate": float(r),
            "due": due
        })
        total += due
    return rows, total

def fixed_rows(items):
    """
    items: list of dicts like {'label': 'Water Loss Levy', 'units': 0.0, 'rate': 25.0, 'amount': None}
    If 'amount' is None, compute amount = units * rate; else use provided amount.
    Returns (rows, total).
    """
    rows = []
    total = 0.0
    for it in (items or []):
        units = float(it.get("units", 0.0))
        rate = float(it.get("rate", 0.0))
        amount = it.get("amount", None)
        if amount is None:
            amount = units * rate
        amount = float(amount)
        rows.append({
            "label": it.get("label", ""),
            "cons": units,
            "rate": rate,
            "due": amount
        })
        total += amount
    return rows, total

def ws_tier_breakdown(cons_kl, days, rates, caps_per_day=None, ws_fixed=None):
    """
    WS: no reduction factors; price per tier directly.
    rates: [r1, r2, r3, r4]
    ws_fixed: list of fixed-line dicts for WS (see fixed_rows)
    Returns dict with rows and totals.
    """
    usages = cumulative_allocate(cons_kl, days, caps_per_day)
    labels = [
        "0–200 L / {} Days".format(days),
        "201–833 L / {} Days".format(days),
        "834–1000 L / {} Days".format(days),
        "1000+ L / {} Days".format(days),
    ]
    tier_list, var_total = tier_rows(labels, usages, rates)
    fixed_list, fixed_total = fixed_rows(ws_fixed)
    total = var_total + fixed_total
    return {
        "tiers": tier_list,
        "fixed": fixed_list,
        "var_total": var_total,
        "total": total,
        "usages": usages  # [u1,u2,u3,u4]
    }

def sd_tier_breakdown(cons_kl, days, rates, reductions, caps_per_day=None, sd_fixed=None):
    """
    SD: apply per-tier reduction factors BEFORE pricing.
    rates: [r1, r2, r3, r4]
    reductions: [f1, f2, f3, f4] like [0.95, 0.75, 0.75, 0.65]
    sd_fixed: list of fixed-line dicts for SD (see fixed_rows)
    """
    usages_raw = cumulative_allocate(cons_kl, days, caps_per_day)
    usages_red = []
    for u, f in zip(usages_raw, reductions):
        usages_red.append(float(u) * float(f))
    labels = [
        "0–200 L / {} Days (reduced)".format(days),
        "201–833 L / {} Days (reduced)".format(days),
        "834–1000 L / {} Days (reduced)".format(days),
        "1000+ L / {} Days (reduced)".format(days),
    ]
    tier_list, var_total = tier_rows(labels, usages_red, rates)
    fixed_list, fixed_total = fixed_rows(sd_fixed)
    total = var_total + fixed_total
    return {
        "tiers": tier_list,
        "fixed": fixed_list,
        "var_total": var_total,
        "total": total,
        "usages_raw": usages_raw,   # before reduction
        "usages_red": usages_red    # after reduction
    }

def electricity_due(cons_kwh, rate_per_kwh):
    """
    Flat electricity: due = consumption * rate.
    """
    return float(cons_kwh) * float(rate_per_kwh)

def water_volume_cost(cons_kl, rate_per_kl):
    """
    Separate water-volume line if your muni bills it outside WS/SD.
    """
    return float(cons_kl) * float(rate_per_kl)

def accumulate_grand(elec_total, ws_sub, sd_sub, water_sub):
    return float(elec_total) + float(ws_sub) + float(sd_sub) + float(water_sub)

def summarize_ws_sd_for_page1(meter_label, ws_total, sd_total, water_total):
    """
    Build the three Page-1 sub-rows payload for a water meter.
    You’ll still render in Jinja; this is just the data.
    """
    return [
        {"kind": "ws", "label": "W&S Cost for {}".format(meter_label), "rate_col": float(ws_total),   "due_col": None},
        {"kind": "sd", "label": "S & D Cost for {}".format(meter_label), "rate_col": float(sd_total), "due_col": None},
        {"kind": "water", "label": "Water Cost for {}".format(meter_label), "rate_col": None,         "due_col": float(water_total)},
    ]

def get_effective_tariff_rate(utility_type, month, code=None):
    qs = BilTariff.query.filter(BilTariff.utility_type == utility_type)
    if code:
        qs = qs.filter(BilTariff.code == code)
    eff_on = _month_start(month)
    # effective_date stored as string 'YYYY-MM-DD'
    # keep <= month start, pick latest
    rec = (qs.filter(BilTariff.effective_date <= eff_on.isoformat())
             .order_by(BilTariff.effective_date.desc())
             .first())
    return float(rec.rate) if rec else None

def _get_tiers(utility_type, month):
    eff_on = _month_start(month)
    tiers = (BilTariff.query
             .filter(BilTariff.utility_type == utility_type,
                     BilTariff.effective_date <= eff_on.isoformat())
             .order_by(BilTariff.block_start.asc())
             .all())
    # Expect rows with block_start/end and rate
    return [{
        "label": f"T{idx+1}",
        "start": float(t.block_start or 0.0),
        "end": float(t.block_end or 0.0) if t.block_end not in (None, 0) else None,
        "rate": float(t.rate)
    } for idx, t in enumerate(tiers)]

def _split_by_tiers_kL(cons_kL, tiers, reductions=None):
    """Return list of {label, cons, rate, due} per tier."""
    remaining = float(cons_kL)
    out = []
    for i, t in enumerate(tiers, start=1):
        start = t["start"]
        end = t["end"]  # None means open-ended
        width = (end - start) if end is not None else max(0.0, remaining)
        # portion falling in this band:
        portion = min(max(remaining, 0.0), width if end is not None else remaining)
        if portion < 0: portion = 0.0
        remaining -= portion

        # SD reduction (optional)
        red = reductions.get(i, 1.0) if reductions else 1.0
        eff_cons = portion * red

        due = eff_cons * t["rate"]
        out.append({
            "label": t["label"],
            "cons": round(portion, 3),
            "eff_cons": round(eff_cons, 3),
            "rate": t["rate"],
            "due": round(due, 2)
        })
        if remaining <= 0.0001:
            break
    return out

def _month_start(month_str):
    y, m = map(int, month_str.split("-"))
    return date(y, m, 1)

def get_effective_rate(utility_type, code, month):
    eff_on = _month_start(month)
    rec = (BilTariff.query
           .filter(BilTariff.utility_type == utility_type,
                   BilTariff.code == code,
                   BilTariff.effective_date <= eff_on.isoformat())
           .order_by(BilTariff.effective_date.desc())
           .first())
    return float(rec.rate) if rec else None

def build_electricity_rows(cons_rows, month):
    elec_rate = get_effective_rate("electricity", "ElecRate", month)
    total_due = 0.0
    out = []
    for r in cons_rows:
        row = dict(r)
        row["consumption"] = int(row.get("consumption") or 0)
        row["rate"] = elec_rate
        row["due"] = round(row["consumption"] * elec_rate, 2) if elec_rate is not None else None
        if row["due"] is not None:
            total_due += row["due"]
        out.append(row)
    return out, round(total_due, 2), elec_rate

def _tiers_for_period_from_tariffs(utility_type, month, days):
    eff_on = _month_start(month)
    trows = (BilTariff.query
             .filter(BilTariff.utility_type == utility_type,
                     BilTariff.effective_date <= eff_on.isoformat())
             .order_by(BilTariff.block_start.asc())
             .all())
    tiers = []
    for idx, t in enumerate(trows, start=1):
        start_ld = float(t.block_start or 0.0)     # L/day
        end_ld = float(t.block_end or 0.0)         # L/day
        start_kl = (start_ld * days) / 1000.0      # kL over period
        end_kl = (end_ld * days) / 1000.0 if end_ld not in (0.0, None) else None
        tiers.append({
            "idx": idx,
            "label": t.code or f"T{idx}",
            "start_kl": start_kl,
            "end_kl": end_kl,                      # None = open-ended
            "rate": float(t.rate),
        })
    return tiers

def _split_consumption(cons_kl, tiers, reductions=None):
    remaining = float(cons_kl)
    out = []
    for t in tiers:
        # width of this band in kL for the period
        width = (t["end_kl"] - t["start_kl"]) if t["end_kl"] is not None else remaining
        width = max(width, 0.0)
        take = min(remaining, width)
        remaining -= take

        red = 1.0
        if reductions:
            # map by tier index or by code if you prefer
            red = reductions.get(t["label"], reductions.get(t["idx"], 1.0))

        eff_cons = take * red
        due = eff_cons * t["rate"]
        out.append({
            "label": t["label"],
            "cons": round(take, 3),        # raw in this band
            "eff_cons": round(eff_cons, 3),# reduced (SD)
            "rate": t["rate"],
            "due": round(due, 2),
        })
        if remaining <= 1e-9:
            break
    return out

_SD_REDUCTION_BY_CODE = {
    "Tier1_SD": 0.95,
    "Tier2_SD": 0.75,
    "Tier3_SD": 0.75,
    "Tier4_SD": 0.65,
}

def _sd_reductions_map(sd_tiers):
    # Build a lookup usable by _split_consumption (by code and index)
    m = {}
    for t in sd_tiers:
        m[t["label"]] = _SD_REDUCTION_BY_CODE.get(t["label"], 1.0)
        m[t["idx"]]   = _SD_REDUCTION_BY_CODE.get(t["label"], 1.0)
    return m

def _ws_fixed_lines(month):
    fixed = []
    v = get_effective_rate("water", "WSSurcharge", month)
    if v: fixed.append({"label": "Surcharge", "amount": round(v, 2)})
    v = get_effective_rate("water", "WaterLossLevy", month)
    if v: fixed.append({"label": "Water Loss Levy", "amount": round(v, 2)})
    v = get_effective_rate("management", "MgmtFee", month)
    if v: fixed.append({"label": "Monthly Management Fee", "amount": round(v, 2)})
    return fixed

def _sd_fixed_lines(month):
    fixed = []
    v = get_effective_rate("sanitation", "SDSurcharge", month)
    if v: fixed.append({"label": "Surcharge", "amount": round(v, 2)})
    v = get_effective_rate("refuse", "RefuseBin", month)
    if v: fixed.append({"label": "Refuse Bins", "amount": round(v, 2)})
    return fixed

def build_water_details_and_rows(water_cons_rows, month):
    detail = []
    page1_rows = []
    grand_total_water = 0.0

    for r in water_cons_rows:
        cons = int(r.get("consumption") or 0)  # kL over period
        days = int(r.get("days") or 0)

        # Period-specific tiers from your L/day bands:
        ws_tiers = _tiers_for_period_from_tariffs("water", month, days)
        sd_tiers = _tiers_for_period_from_tariffs("sanitation", month, days)

        ws_splits = _split_consumption(cons, ws_tiers, reductions=None)
        sd_splits = _split_consumption(cons, sd_tiers, reductions=_sd_reductions_map(sd_tiers))

        ws_fixed = _ws_fixed_lines(month)
        sd_fixed = _sd_fixed_lines(month)

        ws_total = round(sum(t["due"] for t in ws_splits) + sum(f["amount"] for f in ws_fixed), 2)
        sd_total = round(sum(t["due"] for t in sd_splits) + sum(f["amount"] for f in sd_fixed), 2)
        water_total = round(ws_total + sd_total, 2)
        grand_total_water += water_total

        # Page 2 object
        detail.append({
            "meter_label": r["meter_label"],
            "days": days,
            "consumption": cons,
            "ws": {"tiers": ws_splits, "fixed": ws_fixed, "total": ws_total},
            "sd": {"tiers": sd_splits, "fixed": sd_fixed, "total": sd_total},
        })

        # Page 1 rows: base line then 3 summary lines
        base = dict(r)
        base["consumption"] = cons
        base["rate"] = None
        base["due"] = None
        page1_rows.append(base)
        page1_rows.append({"meter_label": f"W&S Cost for {r['meter_label']}",
                           "ws_amount": ws_total, "sd_amount": None, "water_amount": None})
        page1_rows.append({"meter_label": f"S & D Cost for {r['meter_label']}",
                           "ws_amount": None, "sd_amount": sd_total, "water_amount": None})
        page1_rows.append({"meter_label": f"Water Cost for {r['meter_label']}",
                           "ws_amount": None, "sd_amount": None, "water_amount": water_total})

    return detail, page1_rows, round(grand_total_water, 2)

def build_page1_rows(elec_rows, water_rows, month):
    # ELECTRICITY (unchanged idea)
    rows_e, elec_total = [], 0.0
    elec_rate = get_electricity_rate_for_month(month)  # None if missing
    for e in elec_rows:
        due = round((e["consumption"] or 0) * elec_rate, 2) if elec_rate is not None else None
        rows_e.append({
            "kind": "electric",
            **e,
            "rate": elec_rate,
            "due": due,
        })
        elec_total += due or 0.0

    # WATER (the important part)
    rows_w, water_total = [], 0.0
    for w in water_rows:
        # 1) base water line
        rows_w.append({
            "kind": "water-base",
            **w,           # keeps meter_label, dates, readings, days, consumption
            "rate": None,
            "due": None,
        })

        # 2) compute totals ONCE for this meter (tiers + fixed charges)
        ws_amt, sd_amt, water_amt = calc_ws_sd_totals(
            consumption=w["consumption"] or 0,
            days=w["days"] or 0,
            month=month,
            meter_id=w["meter_id"],
        )

        # 3) three summary lines (exactly once)
        rows_w.append({
            "kind": "ws",
            "meter_label": w["meter_label"],
            "rate": round(ws_amt, 2),
            "due": None,
        })
        rows_w.append({
            "kind": "sd",
            "meter_label": w["meter_label"],
            "rate": round(sd_amt, 2),
            "due": None,
        })
        rows_w.append({
            "kind": "water-total",
            "meter_label": w["meter_label"],
            "rate": None,
            "due": round(water_amt, 2),
        })

        water_total += water_amt or 0.0

    due_to_metro = round(elec_total + water_total, 2)
    return rows_e, elec_total, rows_w, water_total, due_to_metro

# ---- METSOA helpers (electricity first) -------------------------------------


def _first_day(month_str: str) -> datetime:
    # month_str format: 'YYYY-MM'
    return datetime.strptime(month_str + "-01", "%Y-%m-%d")

def get_electricity_rate_for_month(month_str: str) -> float | None:
    """Return the electricity flat rate (float) effective for the given month, or None."""
    month_start = _first_day(month_str).date()
    row = (
        db.session.query(BilTariff)
        .filter(
            BilTariff.utility_type == "electricity",
            BilTariff.effective_date <= month_start.isoformat(),
        )
        .order_by(BilTariff.effective_date.desc())
        .first()
    )
    return float(row.rate) if row else None

def get_metsoa_consumption_split(tenant_id, month_str):
    """
    Build (elec_rows, water_rows) for a tenant+month from BilConsumption,
    classified by each meter's utility_type. Electricity rows include rate/due.
    Water rows are base lines (rate/due blank) to be complemented by WS/SD.
    """
    tenant = db.session.get(BilTenant, tenant_id)
    if not tenant:
        return [], []

    # All meters for the tenant's sectional unit -> map number -> type
    meters = BilMeter.query.filter_by(sectional_unit_id=tenant.sectional_unit_id).all()
    meter_nums = [m.meter_number for m in meters]
    type_map = {m.meter_number: (m.utility_type or "").strip().lower() for m in meters}

    if not meter_nums:
        return [], []

    # Pull consumption WITHOUT joining to bil_meter (prevents dropping rows)
    cons_rows = (
        db.session.query(
            BilConsumption.meter_number,
            BilConsumption.last_date,
            BilConsumption.new_date,
            BilConsumption.last_read,
            BilConsumption.new_read,
            BilConsumption.days,
            BilConsumption.consumption,
        )
        .filter(
            BilConsumption.month == month_str,
            BilConsumption.meter_number.in_(meter_nums),
        )
        .order_by(BilConsumption.meter_number.asc())
        .all()
    )

    # Electricity rate (use your helper if present; otherwise fallback)
    try:
        elec_rate = get_electricity_rate_for_month(month_str)
    except NameError:
        month_floor = f"{month_str}-01"
        t = (
            BilTariff.query
            .filter(BilTariff.utility_type == "electricity",
                    BilTariff.effective_date <= month_floor)
            .order_by(BilTariff.effective_date.desc())
            .first()
        )
        elec_rate = float(t.rate) if t else None

    def is_electric(u):
        u = (u or "").lower()
        return u.startswith("elec") or u in {"elec", "electric", "electricity", "power"}

    elec_rows, water_rows = [], []

    for r in cons_rows:
        base = {
            "meter_label": r.meter_number,
            "prev_date": r.last_date,
            "prev_value": int(r.last_read) if r.last_read is not None else None,
            "curr_date": r.new_date,
            "curr_value": int(r.new_read) if r.new_read is not None else None,
            "days": int(r.days) if r.days is not None else None,
            "consumption": int(r.consumption) if r.consumption is not None else None,
        }

        utype = type_map.get(r.meter_number, "")
        if is_electric(utype):
            due = None
            if elec_rate is not None and base["consumption"] is not None:
                due = round(base["consumption"] * elec_rate, 2)
            base.update({"rate": elec_rate, "due": due})
            elec_rows.append(base)
        else:
            # Treat everything not clearly electricity as Water family for Page 1.
            base.update({"rate": None, "due": None})
            water_rows.append(base)

    return elec_rows, water_rows

def build_page1_from_consumption(tenant_id: int, month_str: str):
    """
    One-pass builder from bil_consumption -> Page 1 rows.
    - Electricity rows get rate & due immediately (from tariff).
    - Water rows: add base line, compute WS/SD, then add the 3 summary lines.
    Returns: (elec_rows, elec_total, water_rows, due_to_metro)
    """
    tenant = db.session.get(BilTenant, tenant_id)
    if not tenant:
        return [], 0.0, [], 0.0

    # 1) Get this tenant's meters and a type map (by meter_number).
    meters = BilMeter.query.filter_by(sectional_unit_id=tenant.sectional_unit_id).all()
    meter_nums = [m.meter_number for m in meters]
    type_map = {m.meter_number: (m.utility_type or "").strip().lower() for m in meters}
    if not meter_nums:
        return [], 0.0, [], 0.0

    # 2) Pull consumption rows for month for ONLY those meter numbers (no join to avoid drops).
    cons_rows = (
        db.session.query(
            BilConsumption.meter_id,
            BilConsumption.meter_number,
            BilConsumption.last_date,
            BilConsumption.new_date,
            BilConsumption.last_read,
            BilConsumption.new_read,
            BilConsumption.days,
            BilConsumption.consumption,
        )
        .filter(
            BilConsumption.month == month_str,
            BilConsumption.meter_number.in_(meter_nums),
        )
        .order_by(BilConsumption.meter_number.asc())
        .all()
    )

    # 3) Classifier (tolerant): anything not clearly electricity is treated as water-family for Page 1.
    def is_electric(u: str) -> bool:
        u = (u or "").lower()
        return u.startswith("elec") or u in {"elec", "electric", "electricity", "power"}

    elec_rate = get_electricity_rate_for_month(month_str)  # may be None

    elec_rows = []
    water_rows = []
    elec_total = 0.0
    due_to_metro = 0.0

    # 4) Walk every consumption row once; append Page-1 rows as we go.
    for r in cons_rows:
        utype = type_map.get(r.meter_number, "")
        base = {
            "meter_label": r.meter_number,
            "prev_date": r.last_date,
            "prev_value": int(r.last_read) if r.last_read is not None else None,
            "curr_date": r.new_date,
            "curr_value": int(r.new_read) if r.new_read is not None else None,
            "days": int(r.days) if r.days is not None else None,
            "consumption": int(r.consumption) if r.consumption is not None else None,
        }

        if is_electric(utype):
            # Electricity: compute due immediately.
            due = None
            if elec_rate is not None and base["consumption"] is not None:
                due = round(base["consumption"] * elec_rate, 2)
                elec_total += due
                due_to_metro += due
            base.update({"rate": elec_rate, "due": due})
            elec_rows.append(base)
        else:
            # Water-family: 4 lines per meter.
            # (1) Base line with blanks in Rate/Due:
            water_rows.append({**base, "kind": "water-base", "rate": None, "due": None})

            # (2) Compute WS/SD/Total once for this meter, using your tier function.
            ws_amt, sd_amt, water_amt = calc_ws_sd_totals(
                consumption=base["consumption"] or 0,
                days=base["days"] or 0,
                month=month_str,
                meter_id=r.meter_id,
            )

            # (3) Add the three summary lines (WS, SD, Water Cost):
            water_rows.append({
                "kind": "ws", "meter_label": base["meter_label"],
                "rate": round(ws_amt, 2), "due": None
            })
            water_rows.append({
                "kind": "sd", "meter_label": base["meter_label"],
                "rate": round(sd_amt, 2), "due": None
            })
            water_rows.append({
                "kind": "water-total", "meter_label": base["meter_label"],
                "rate": None, "due": round(water_amt, 2)
            })

            # (4) Update grand total:
            due_to_metro += water_amt or 0.0

    return elec_rows, round(elec_total, 2), water_rows, round(due_to_metro, 2)


# app/utils/billing_helpers.py
#
# Helpers that transform the already-stored consumption (bil_consumption)
# into METSOA Page 1 rows and totals.
#
# Assumptions:
# - bil_consumption has integer consumption (no decimals).
# - bil_tariff holds electricity flat rate and tiered water/sanitation + fixed items.
# - BilTenant.sectional_unit_id -> BilMeter.sectional_unit_id maps tenant to meters.
# - BilMeter.utility_type in {"electricity","water"} (sanitation/refuse are tariffs only).
#
# Returned row shapes for Page 1:
# ELEC row:
#   {"kind":"elec","meter":..., "prev_date":..., "prev_value":..., "curr_date":..., "curr_value":...,
#    "days":..., "consumption":..., "rate": float, "due": float}
#
# WATER rows (4 lines per meter):
#   1) consumption display row:
#      {"kind":"water-cons","meter":..., dates/values..., "days":..., "consumption":..., "rate": None, "due": None}
#   2) WS:
#      {"kind":"ws","label":"W&S Cost for XXX","rate": float, "due": None}
#   3) SD:
#      {"kind":"sd","label":"S & D Cost for XXX","rate": float, "due": None}
#   4) TOTAL:
#      {"kind":"water-total","label":"Water Cost for XXX","rate": None, "due": float}
#
# Totals:
#   elec_total: float
#   water_total: float
#   due_to_metro = elec_total + water_total



# ---------- small utilities ----------


def _to_two(x):
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _yymm(month_str):
    """month_str 'YYYY-MM' -> (year:int, month:int)"""
    y, m = month_str.split("-")
    return int(y), int(m)


def _first_of_month(month_str):
    y, m = _yymm(month_str)
    return date(y, m, 1)


# ---------- tariff lookup ----------

def get_electricity_rate_for_month(month_str):
    """Flat electricity rate effective for that month (latest <= month-start)."""
    eff = _first_of_month(month_str).isoformat()
    q = (
        db.session.query(BilTariff)
        .filter(BilTariff.utility_type == "electricity")
        .filter(BilTariff.effective_date <= eff)
        .order_by(BilTariff.effective_date.desc(), BilTariff.id.desc())
    )
    row = q.first()
    return float(row.rate) if row else None


def _get_fixed_amount(code, month_str):
    """Fixed tariff by code (MgmtFee, WaterLossLevy, WSSurcharge, SDSurcharge, RefuseBin, etc.)."""
    eff = _first_of_month(month_str).isoformat()
    t = (
        db.session.query(BilTariff)
        .filter(BilTariff.code == code)
        .filter(BilTariff.effective_date <= eff)
        .order_by(BilTariff.effective_date.desc(), BilTariff.id.desc())
        .first()
    )
    return float(t.rate) if t else 0.0


def _get_tier_bands(utility_type, month_str):
    """
    For 'water' and 'sanitation' return tier bands keyed by liters/day cutoffs.
    We use your municipal scheme (0–200, 201–833, 834–1000, 1001–1500 L/day).
    Rates are per kL.

    Returns list of dicts in order:
    [{"start":0, "end":200, "rate":...}, ...]
    """
    eff = _first_of_month(month_str).isoformat()
    q = (
        db.session.query(BilTariff)
        .filter(BilTariff.utility_type == utility_type)
        .filter(BilTariff.block_end > 0)    # tiers only
        .filter(BilTariff.effective_date <= eff)
        .order_by(BilTariff.block_start.asc())
    )
    bands = []
    for t in q:
        bands.append({
            "start": int(t.block_start),    # L/day
            "end": int(t.block_end),        # L/day
            "rate": float(t.rate),          # per kL
        })
    return bands


# ---------- core calculators ----------

def calc_electric_due(consumption_units, month_str):
    """ELEC is flat: due = kWh * rate."""
    rate = get_electricity_rate_for_month(month_str)
    if rate is None:
        return 0.0, None  # no rate configured
    due = _to_two(consumption_units * rate)
    return due, rate


def _split_daily_into_tiers(avg_l_per_day, bands):
    """
    Given average L/day and tier bands, return liters/day in each tier.
    bands: [{"start":0,"end":200,"rate":...}, ...] (inclusive ranges).
    """
    remain = max(0.0, float(avg_l_per_day))
    out = []
    prev_end = -1
    for b in bands:
        width = max(0.0, b["end"] - b["start"])  # e.g. 200, 633, 166, 500
        take = min(remain, width)
        out.append(take)
        remain -= take
    # anything above last end is ignored (municipality caps)
    return out  # liters/day per band (not kL/day)


def build_metsoa_page2_breakdown(tenant_id, month_str):
    """
    Build per-water-meter detailed tier breakdown for Page 2.
    Returns a list of dicts, one per water meter:
    {
      "meter": "AGN489",
      "prev_date": ..., "prev_value": int, "curr_date": ..., "curr_value": int,
      "days": int, "consumption": int,
      "ws": {
        "tiers": [{"label": str, "cons": float, "rate": float, "due": float}, ...],
        "fixed": [{"label": str, "cons": 0, "rate": float, "due": float}, ...],
        "total": float,
      },
      "sd": { ... same shape ... }
    }
    """
    tenant = db.session.get(BilTenant, tenant_id)
    if not tenant:
        return []

    meters = (
        db.session.query(BilMeter)
        .filter(BilMeter.sectional_unit_id == tenant.sectional_unit_id)
        .filter(BilMeter.utility_type == "water")
        .order_by(BilMeter.id.asc())
        .all()
    )
    meters_by_id = {m.id: m for m in meters}
    if not meters_by_id:
        return []

    cons_rows = (
        db.session.query(BilConsumption)
        .filter(BilConsumption.month == month_str)
        .filter(BilConsumption.meter_id.in_(list(meters_by_id.keys())))
        .order_by(BilConsumption.meter_id.asc())
        .all()
    )

    water_bands = _get_tier_bands("water", month_str)
    sd_bands = _get_tier_bands("sanitation", month_str)
    # municipal sanitation reduction factors (band-wise)
    factors = [0.95, 0.75, 0.75, 0.65]

    # fixed charges (monthly, shown in the “Rate” col, applied into totals)
    ws_fixed_lines = [
        ("Surcharge", _get_fixed_amount("WSSurcharge", month_str)),
        ("Water Loss Levy", _get_fixed_amount("WaterLossLevy", month_str)),
        ("Monthly Management Fee", _get_fixed_amount("MgmtFee", month_str)),
    ]
    sd_fixed_lines = [
        ("Surcharge", _get_fixed_amount("SDSurcharge", month_str)),
        ("Refuse Bins", _get_fixed_amount("RefuseBin", month_str)),
    ]

    out = []
    for r in cons_rows:
        m = meters_by_id.get(r.meter_id)
        if not m:
            continue

        days = max(1, int(r.days))
        cons_kl = int(r.consumption)  # integer consumption requirement
        avg_lpd = (float(cons_kl) * 1000.0) / days

        # --- WATER (WS) tiers ---
        ws_tiers = []
        ws_amount = 0.0
        if water_bands:
            lpd_each = _split_daily_into_tiers(avg_lpd, water_bands)
            for i, lpd in enumerate(lpd_each):
                if lpd <= 0:
                    continue
                band = water_bands[i]
                kl_in_band = (lpd * days) / 1000.0
                due = kl_in_band * band["rate"]
                ws_tiers.append({
                    "label": f"{band['start']}L–{band['end']}L / {days} Days",
                    "cons": kl_in_band,
                    "rate": band["rate"],
                    "due": _to_two(due),
                })
                ws_amount += due

        # --- SANITATION (SD) tiers with reduction factors ---
        sd_tiers = []
        sd_amount = 0.0
        if sd_bands:
            lpd_each_sd = _split_daily_into_tiers(avg_lpd, sd_bands)
            for i, lpd in enumerate(lpd_each_sd):
                if lpd <= 0:
                    continue
                band = sd_bands[i]
                factor = factors[i] if i < len(factors) else factors[-1]
                kl_in_band = ((lpd * factor) * days) / 1000.0
                due = kl_in_band * band["rate"]
                sd_tiers.append({
                    "label": f"{band['start']}L–{band['end']}L / {days} Days",
                    "cons": kl_in_band,
                    "rate": band["rate"],
                    "due": _to_two(due),
                })
                sd_amount += due

        # fixed lines
        ws_fixed_total = sum(rate for _, rate in ws_fixed_lines)
        sd_fixed_total = sum(rate for _, rate in sd_fixed_lines)

        ws_total = _to_two(ws_amount + ws_fixed_total)
        sd_total = _to_two(sd_amount + sd_fixed_total)

        out.append({
            "meter": m.meter_number,
            "prev_date": r.last_date,
            "prev_value": int(r.last_read),
            "curr_date": r.new_date,
            "curr_value": int(r.new_read),
            "days": days,
            "consumption": cons_kl,
            "ws": {
                "tiers": ws_tiers,
                "fixed": [{"label": lbl, "cons": 0, "rate": amt, "due": _to_two(amt)} for lbl, amt in ws_fixed_lines],
                "total": ws_total,
            },
            "sd": {
                "tiers": sd_tiers,
                "fixed": [{"label": lbl, "cons": 0, "rate": amt, "due": _to_two(amt)} for lbl, amt in sd_fixed_lines],
                "total": sd_total,
            },
        })

    return out


# NOTE: add your own imports at the top of the file, e.g.:


# ──────────────────────────────────────────────────────────────────────────────
# Small internal helpers (pure functions; safe to test)
# ──────────────────────────────────────────────────────────────────────────────

def _int_or_none(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None


def _money_or_none(x):
    return float(x) if x is not None else None


def _default_sd_reduction(code):
    # Fallback if reduction_factor is null on sanitation tiers
    code = (code or "").upper()
    if "TIER1_SD" in code:
        return 0.95
    if "TIER2_SD" in code or "TIER3_SD" in code:
        return 0.75
    if "TIER4_SD" in code:
        return 0.65
    return 1.00


def _fetch_tariff_rate(db, BilTariff, utility_type, code, month_str):
    """Return dict(rate, reduction_factor, unit) for the latest tariff <= month_end."""
    _, month_end = _month_bounds(month_str)
    q = (db.session.query(BilTariff)
         .filter(BilTariff.utility_type == utility_type)
         .filter(BilTariff.code == code)
         .filter(BilTariff.effective_date <= month_end)
         .order_by(BilTariff.effective_date.desc()))
    t = q.first()
    if not t:
        return {"rate": None, "reduction_factor": None, "unit": None}
    return {
        "rate": _money_or_none(t.rate),
        "reduction_factor": _money_or_none(getattr(t, "reduction_factor", None)),
        "unit": getattr(t, "unit", None),
    }


def _active_map_rows_for_meter(db, BilMeterChargeMap, meter_id, month_str):
    """All active map rows for a meter in the month."""
    month_start, month_end = _month_bounds(month_str)
    q = (db.session.query(BilMeterChargeMap)
         .filter(BilMeterChargeMap.meter_id == meter_id)
         .filter(BilMeterChargeMap.is_enabled == 1)
         .filter(BilMeterChargeMap.effective_start <= month_end)
         .filter((BilMeterChargeMap.effective_end == None) | (BilMeterChargeMap.effective_end >= month_start)))
    rows = []
    for m in q.all():
        rows.append({
            "charge_code": m.charge_code,
            "utility_type": getattr(m, "utility_type", None),
            "tariff_code_override": getattr(m, "tariff_code_override", None),
            "action": getattr(m, "action", "include"),
            "bill_to": getattr(m, "bill_to", "tenant"),
            "show_on_tenant": int(getattr(m, "show_on_tenant", 1)),
        })
    return rows


def _get_tenant_and_meters(db, BilTenant, BilMeter, tenant_id):
    tenant = db.session.get(BilTenant, tenant_id)
    if not tenant:
        return None, []
    unit_id = tenant.sectional_unit_id
    meters = (db.session.query(BilMeter)
              .filter(BilMeter.sectional_unit_id == unit_id)
              .order_by(BilMeter.utility_type.asc(), BilMeter.meter_number.asc())
              .all())
    return tenant, meters


def _cons_rows_for_tenant_month(db, BilConsumption, meters, month_str):
    """Fetch consumption rows for the tenant's meters in the given month."""
    meter_ids = [m.id for m in meters]
    if not meter_ids:
        return []
    q = (db.session.query(BilConsumption)
         .filter(BilConsumption.month == month_str)
         .filter(BilConsumption.meter_id.in_(meter_ids))
         .order_by(BilConsumption.meter_id.asc()))
    return q.all()


# ──────────────────────────────────────────────────────────────────────────────
# Tier allocation for water & sanitation
# ──────────────────────────────────────────────────────────────────────────────

def _load_tiers(db, BilTariff, utility_type, month_str):
    """Load all rows for utility_type ('water' or 'sanitation') that look like tiers, sorted by block_start."""
    _, month_end = _month_bounds(month_str)
    q = (db.session.query(BilTariff)
         .filter(BilTariff.utility_type == utility_type)
         .filter(BilTariff.effective_date <= month_end)
         .order_by(BilTariff.block_start.asc(), BilTariff.block_end.asc()))
    tiers = []
    for t in q.all():
        # Only keep tier-like rows (codes starting with Tier*)
        if not (t.code or "").lower().startswith("tier"):
            continue
        tiers.append({
            "code": t.code,
            "rate": _money_or_none(t.rate),
            "unit": getattr(t, "unit", None),
            # interpret block_* as liters/day thresholds if they are large numbers; otherwise treat as kL/day
            "block_start": float(getattr(t, "block_start", 0.0) or 0.0),
            "block_end": float(getattr(t, "block_end", 0.0) or 0.0),
            "reduction_factor": _money_or_none(getattr(t, "reduction_factor", None)),
        })
    return tiers


def _alloc_by_tiers(cons_kl, days, tiers, is_sd=False):
    """
    Allocate consumption (kL in the month) across tier caps.
    Assumes tariff.block_* are liters/day thresholds. Cap per tier = (block_end - block_start)/1000 * days.
    For sanitation (is_sd=True), we apply reduction per tier (tariff or default) to the allocated kL.
    Returns: (alloc_rows, subtotal)
      alloc_rows: [{tier_code, vol_kl, rate, due, reduction_applied}]
    """
    remaining = max(0.0, float(cons_kl or 0))
    rows = []
    subtotal = 0.0

    for t in tiers:
        # compute tier cap in kL for the period
        cap_kl = 0.0
        if t["block_end"] and t["block_end"] > t["block_start"]:
            cap_kl = ((t["block_end"] - t["block_start"]) / 1000.0) * float(days or 0)
        else:
            # last/open tier → no finite cap (use all remaining)
            cap_kl = remaining

        take = min(remaining, cap_kl)
        if take <= 0.0:
            # nothing left to allocate
            # still append a zero row so breakdown shows all tiers if you like; optional
            rows.append({
                "tier_code": t["code"],
                "vol_kl": 0.0,
                "rate": _money_or_none(t["rate"]),
                "due": 0.0,
                "reduction_applied": 1.0 if not is_sd else (t["reduction_factor"] or _default_sd_reduction(t["code"])),
            })
            continue

        rate = _money_or_none(t["rate"]) or 0.0
        if is_sd:
            rf = t["reduction_factor"] or _default_sd_reduction(t["code"])
            bill_kl = take * rf
        else:
            rf = 1.0
            bill_kl = take

        due = bill_kl * rate
        subtotal += due
        rows.append({
            "tier_code": t["code"],
            "vol_kl": round(take, 3),
            "rate": rate,
            "due": round(due, 2),
            "reduction_applied": rf,
        })
        remaining -= take

    return rows, round(subtotal, 2)


# ──────────────────────────────────────────────────────────────────────────────
# Water costs for a single meter (tiers + mapped extras)
# ──────────────────────────────────────────────────────────────────────────────

def compute_water_costs_for_meter(db, BilTariff, BilMeterChargeMap, meter_id, cons_kl, days, month_str):
    """
    Returns a dict:
      {
        'ws_rows': [...], 'ws_subtotal': float,
        'sd_rows': [...], 'sd_subtotal': float,
        'extra_rows': [...],            # surcharges/levies/fees resolved via map+tariff
        'ws_cost': float,               # ws_subtotal + ws-side extras
        'sd_cost': float,               # sd_subtotal + sd-side extras
        'water_cost': float             # ws_cost + sd_cost
      }
    """
    water_tiers = _load_tiers(db, BilTariff, "water", month_str)
    sd_tiers    = _load_tiers(db, BilTariff, "sanitation", month_str)

    ws_rows, ws_sub = _alloc_by_tiers(cons_kl, days, water_tiers, is_sd=False)
    sd_rows, sd_sub = _alloc_by_tiers(cons_kl, days, sd_tiers,    is_sd=True)

    # extras by map
    map_rows = _active_map_rows_for_meter(db, BilMeterChargeMap, meter_id, month_str)
    extra_rows = []
    ws_extra = 0.0
    sd_extra = 0.0

    # precompute total reduced kL for SD tiers (sum of billable kL actually charged on SD tiers)
    total_sd_bill_kl = 0.0
    for r in sd_rows:
        rf = r.get("reduction_applied", 1.0) or 1.0
        total_sd_bill_kl += (r.get("vol_kl", 0.0) or 0.0) * rf

    # map → tariff → compute extra line
    for mr in map_rows:
        if (mr.get("action") or "include") != "include":
            continue

        code = mr.get("tariff_code_override") or mr.get("charge_code")
        utyp = mr.get("utility_type") or ""
        tar  = _fetch_tariff_rate(db, BilTariff, utyp, code, month_str)
        rate = tar["rate"]
        unit = (tar["unit"] or "").lower()

        if rate is None:
            continue

        # decide quantity
        qty = 0.0
        label = code

        # Quantity rules:
        # - Surcharges with unit 'kL' multiply by kL (full cons for WSSurcharge, reduced for SDSurcharge)
        # - Flat (ZAR/month) items add as-is
        if unit == "kl":
            if code.upper().startswith("WSSURCHARGE"):
                qty = float(cons_kl or 0.0)
            elif code.upper().startswith("SDSURCHARGE"):
                qty = float(total_sd_bill_kl or 0.0)
            else:
                # any other per-kL item → use full consumption
                qty = float(cons_kl or 0.0)
        else:
            # flat monthly
            qty = 1.0

        amount = rate * qty
        extra_rows.append({
            "code": code,
            "utility_type": utyp,
            "rate": rate,
            "qty": round(qty, 3),
            "amount": round(amount, 2),
        })

        # assign to WS or SD buckets
        if utyp == "water":
            ws_extra += amount
        elif utyp == "sanitation":
            sd_extra += amount
        else:
            # management/refuse/etc → add to total on the WS side by convention (you can change)
            ws_extra += amount

    ws_cost = round(ws_sub + ws_extra, 2)
    sd_cost = round(sd_sub + sd_extra, 2)
    water_cost = round(ws_cost + sd_cost, 2)

    return {
        "ws_rows": ws_rows,
        "ws_subtotal": round(ws_sub, 2),
        "sd_rows": sd_rows,
        "sd_subtotal": round(sd_sub, 2),
        "extra_rows": extra_rows,
        "ws_cost": ws_cost,
        "sd_cost": sd_cost,
        "water_cost": water_cost,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Page 1: build combined rows (elec + water with WS/SD lines per water meter)
# ──────────────────────────────────────────────────────────────────────────────

def split_rows_for_metsoa(db, BilTenant, BilMeter, BilConsumption, BilTariff, BilMeterChargeMap, tenant_id, month_str):
    """
    Returns:
      rows: list of dicts for Page 1 table (in final order: elec rows, Elec Subtotal line, water blocks)
      elec_total: float
      due_to_metro: float
      tenant: BilTenant
      month: month_str
    Each 'row' dict has keys:
      kind: 'elec'|'water'|'divider'|'ws_line'|'sd_line'|'water_cost'
      meter_id, meter_label, prev_date, prev_value, curr_date, curr_value, days, consumption, rate, due, note
    """
    tenant, meters = _get_tenant_and_meters(db, BilTenant, BilMeter, tenant_id)
    if not tenant:
        return {"rows": [], "elec_total": 0.0, "due_to_metro": 0.0, "tenant": None, "month": month_str}

    m_by_id = {m.id: m for m in meters}
    cons_rows = _cons_rows_for_tenant_month(db, BilConsumption, meters, month_str)

    # fast lookup: utility type by meter
    def _utype(mid):
        m = m_by_id.get(mid)
        return (m.utility_type.lower() if m and m.utility_type else "").lower()

    rows = []
    elec_total = 0.0
    due_to_metro = 0.0
    saw_electricity = False
    saw_water = False

    # elec rate lookup (flat)
    ele_rate_info = _fetch_tariff_rate(db, BilTariff, "electricity", "ElecRate", month_str)
    ele_rate = ele_rate_info["rate"]

    # First pass: we’ll collect electric and water into two buckets to insert the divider in between
    elec_rows_tmp = []
    water_blocks_tmp = []

    for r in cons_rows:
        mid = r.meter_id
        m = m_by_id.get(mid)
        label = m.meter_number if m else (getattr(r, "meter_number", None) or f"M{mid}")

        prev_date = r.last_date
        curr_date = r.new_date
        prev_value = _int_or_none(r.last_read)
        curr_value = _int_or_none(r.new_read)
        days = _int_or_none(r.days)
        cons = _int_or_none(r.consumption) or 0

        utyp = _utype(mid)

        if utyp == "electricity":
            saw_electricity = True
            rate = ele_rate
            due = round((cons or 0) * (rate or 0.0), 2) if (rate is not None) else None
            elec_total += (due or 0.0)

            elec_rows_tmp.append({
                "kind": "elec",
                "meter_id": mid,
                "meter_label": label,
                "prev_date": prev_date,
                "prev_value": prev_value,
                "curr_date": curr_date,
                "curr_value": curr_value,
                "days": days,
                "consumption": cons,
                "rate": rate,
                "due": due,
                "note": None,
            })

            due_to_metro += (due or 0.0)

        elif utyp == "water":
            saw_water = True
            # base water line (blank Rate/Due on Page 1)
            block = []
            block.append({
                "kind": "water",
                "meter_id": mid,
                "meter_label": label,
                "prev_date": prev_date,
                "prev_value": prev_value,
                "curr_date": curr_date,
                "curr_value": curr_value,
                "days": days,
                "consumption": cons,
                "rate": None,
                "due": None,
                "note": None,
            })

            # compute WS/SD totals + extras for this meter
            wc = compute_water_costs_for_meter(db, BilTariff, BilMeterChargeMap, mid, cons, days, month_str)

            # Insert WS subtotal (Rate column), SD subtotal (Rate column), and the combined Water Cost (Due column)
            block.append({
                "kind": "ws_line",
                "meter_id": mid,
                "meter_label": f"WS Subtotal",
                "prev_date": None, "prev_value": None, "curr_date": None, "curr_value": None, "days": None, "consumption": None,
                "rate": wc["ws_cost"],   # Page 1 convention: show WS cost in Rate column
                "due": None,
                "note": None,
            })
            block.append({
                "kind": "sd_line",
                "meter_id": mid,
                "meter_label": f"SD Subtotal",
                "prev_date": None, "prev_value": None, "curr_date": None, "curr_value": None, "days": None, "consumption": None,
                "rate": wc["sd_cost"],   # Page 1 convention: show SD cost in Rate column
                "due": None,
                "note": None,
            })
            block.append({
                "kind": "water_cost",
                "meter_id": mid,
                "meter_label": f"Water Cost for #{label}",
                "prev_date": None, "prev_value": None, "curr_date": None, "curr_value": None, "days": None, "consumption": None,
                "rate": None,
                "due": wc["water_cost"],  # Page 1 convention: combined due in Due column
                "note": None,
            })

            water_blocks_tmp.append(block)
            due_to_metro += wc["water_cost"] or 0.0

        else:
            # Unknown type: still show the base line to avoid surprises
            elec_rows_tmp.append({
                "kind": "elec",
                "meter_id": mid,
                "meter_label": label,
                "prev_date": prev_date,
                "prev_value": prev_value,
                "curr_date": curr_date,
                "curr_value": curr_value,
                "days": days,
                "consumption": cons,
                "rate": None,
                "due": None,
                "note": "Unknown utility type",
            })

    # Assemble final order: all ELEC rows, then a divider line, then WATER blocks
    rows.extend(elec_rows_tmp)

    if saw_electricity and saw_water:
        rows.append({
            "kind": "divider",
            "meter_id": None,
            "meter_label": "Electricity Subtotal",
            "prev_date": None, "prev_value": None,
            "curr_date": None, "curr_value": None,
            "days": None, "consumption": None,
            "rate": None,
            "due": round(elec_total, 2),
            "note": None,
        })

    for block in water_blocks_tmp:
        rows.extend(block)

    return {
        "rows": rows,
        "elec_total": round(elec_total, 2),
        "due_to_metro": round(due_to_metro, 2),
        "tenant": tenant,
        "month": month_str,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Page 2: detailed breakdown for a specific WATER meter (tiers + extras)
# ──────────────────────────────────────────────────────────────────────────────

def water_breakdown_for_page2(db, BilConsumption, BilTariff, BilMeterChargeMap, meter_id, month_str):
    """
    Return dict to render Page 2 for one water meter in a month:
      {
        'meter_id': ...,
        'label': ...,
        'days': int,
        'consumption': int,
        'ws_rows': [...], 'ws_subtotal': float,
        'sd_rows': [...], 'sd_subtotal': float,
        'extra_rows': [...],
        'ws_cost': float, 'sd_cost': float, 'water_cost': float
      }
    """
    r = (db.session.query(BilConsumption)
         .filter(BilConsumption.month == month_str)
         .filter(BilConsumption.meter_id == meter_id)
         .first())
    if not r:
        return None

    cons = _int_or_none(r.consumption) or 0
    days = _int_or_none(r.days) or 0
    label = getattr(r, "meter_number", None) or f"M{meter_id}"

    wc = compute_water_costs_for_meter(db, BilTariff, BilMeterChargeMap, meter_id, cons, days, month_str)

    return {
        "meter_id": meter_id,
        "label": label,
        "days": days,
        "consumption": cons,
        "ws_rows": wc["ws_rows"],
        "ws_subtotal": wc["ws_subtotal"],
        "sd_rows": wc["sd_rows"],
        "sd_subtotal": wc["sd_subtotal"],
        "extra_rows": wc["extra_rows"],
        "ws_cost": wc["ws_cost"],
        "sd_cost": wc["sd_cost"],
        "water_cost": wc["water_cost"],
    }


def _mapped_totals_for_meter(meter_id, month_str, cons_kl):
    """
    Sum up extra fixed/variable charges driven by the map.
    - If tariff.unit contains 'kL' OR the code is a *Surcharge*, treat as per-kL (qty = consumption).
    - Otherwise treat as fixed monthly.
    Returns (ws_total, sd_total).
    """
    rows = db.session.execute(text("""
        SELECT t.utility_type,
               t.code,
               t.rate,
               lower(coalesce(t.unit,'')) AS unit
        FROM bil_meter_charge_map mm
        JOIN bil_tariff t
          ON t.code = mm.charge_code
         AND t.effective_date <= :cut
        WHERE mm.is_enabled = 1
          AND mm.meter_id   = :mid
          AND (mm.effective_start IS NULL OR mm.effective_start <= :d0)
          AND (mm.effective_end   IS NULL OR mm.effective_end   >= :d0)
        ORDER BY mm.id
    """), {"mid": meter_id, "cut": f"{month_str}-28", "d0": f"{month_str}-01"}).mappings().all()

    ws_total = 0.0
    sd_total = 0.0
    qty = float(cons_kl or 0)

    for r in rows:
        unit = r["unit"]
        rate = float(r["rate"] or 0)

        # per-kL if unit says so, or if it’s an explicit surcharge code
        if (unit and "kl" in unit) or r["code"].endswith("Surcharge"):
            amount = rate * qty
        else:
            amount = rate

        if r["utility_type"] == "water":
            ws_total += amount
        elif r["utility_type"] == "sanitation":
            sd_total += amount

    return ws_total, sd_total



# ───────────────────────────────────────────────────────────────────────────────
# Tariff + charge-map helpers (use existing `db` and `text` in your file)
# ───────────────────────────────────────────────────────────────────────────────

def _latest_electric_rate():
    row = db.session.execute(text("""
        SELECT t.rate
        FROM bil_tariff t
        WHERE lower(t.utility_type)='electricity' AND t.code='ElecRate'
        ORDER BY date(t.effective_date) DESC
        LIMIT 1
    """)).fetchone()
    return float(row.rate) if row and row.rate is not None else 0.0


def _latest_tiers(utility_type: str):
    rows = db.session.execute(text("""
        SELECT
          COALESCE(block_start,0)          AS block_start,
          NULLIF(block_end,0)              AS block_end,
          rate,
          COALESCE(reduction_factor,1.0)   AS rf
        FROM bil_tariff
        WHERE lower(utility_type)=:u AND code LIKE 'Tier%%'
        ORDER BY block_start ASC
    """), {"u": utility_type.lower()}).mappings().all()

    out = []
    for r in rows:
        out.append({
            "block_start": float(r["block_start"] or 0.0),
            "block_end":   float(r["block_end"]) if r["block_end"] is not None else None,
            "rate":        float(r["rate"] or 0.0),
            "rf":          float(r["rf"] or 1.0),
        })
    return out


def _apply_tiers(cons_kl: float, tiers, apply_reduction: bool):
    remaining = float(cons_kl or 0.0)
    total_due = 0.0
    lines = []

    for i, t in enumerate(tiers, start=1):
        if remaining <= 0:
            break
        lower = float(t["block_start"] or 0.0)
        upper = t["block_end"]  # may be None
        width = (upper - lower) if (upper is not None) else remaining
        if width <= 0:
            continue

        take = min(remaining, width)
        qty = take * (t["rf"] if apply_reduction else 1.0)
        due = qty * float(t["rate"] or 0.0)
        total_due += due

        lines.append({
            "label": f"Tier {i}" if upper is not None else f"Tier {i}+",
            "qty": round(qty, 3),
            "rate": float(t["rate"] or 0.0),
            "due":  round(due, 2),
        })
        remaining -= take

    if remaining > 0 and tiers:
        last = tiers[-1]
        qty = remaining * (last["rf"] if apply_reduction else 1.0)
        due = qty * float(last["rate"] or 0.0)
        total_due += due
        lines.append({
            "label": f"Tier {len(tiers)}+",
            "qty": round(qty, 3),
            "rate": float(last["rate"] or 0.0),
            "due":  round(due, 2),
        })

    return round(total_due, 2), lines


def _mapped_extras_for_meter(meter_id: int, cons_kl: float):
    """
    Read per-meter charges from bil_meter_charge_map and resolve to latest bil_tariff.
    Per-kL items → qty = cons_kl (× rf if sanitation); fixed items → qty=1.
    Returns (ws_extra, sd_extra, detail_lines).
    """
    maps = db.session.execute(text("""
        SELECT utility_type, charge_code
        FROM bil_meter_charge_map
        WHERE meter_id=:mid AND COALESCE(is_enabled,1)=1
    """), {"mid": meter_id}).mappings().all()

    if not maps:
        return 0.0, 0.0, []

    ws_extra = 0.0
    sd_extra = 0.0
    details = []

    for m in maps:
        util = (m["utility_type"] or "").lower().strip()
        code = (m["charge_code"] or "").strip()
        if not util or not code:
            continue

        t = db.session.execute(text("""
            SELECT rate,
                   COALESCE(reduction_factor,1.0) AS rf,
                   LOWER(COALESCE(unit,''))        AS unit
            FROM bil_tariff
            WHERE utility_type=:util AND code=:code
            ORDER BY date(effective_date) DESC
            LIMIT 1
        """), {"util": util, "code": code}).mappings().first()
        if not t:
            continue

        rate = float(t["rate"] or 0.0)
        rf   = float(t["rf"] or 1.0)
        unit = (t["unit"] or "").lower()

        if unit in ("kl","per_kl") or code.lower().endswith("surcharge"):
            qty = float(cons_kl or 0.0) * (rf if util == "sanitation" else 1.0)
        else:
            qty = 1.0

        due = round(qty * rate, 2)
        details.append({
            "group": "ws" if util == "water" else ("sd" if util == "sanitation" else util),
            "label": code, "qty": round(qty, 3), "rate": rate, "due": due,
        })

        if util == "water":
            ws_extra += due
        elif util == "sanitation":
            sd_extra += due

    return round(ws_extra, 2), round(sd_extra, 2), details


def _consumption_rows_from_table():
    """
    Read the current scope from bil_consumption (already prefiltered upstream by you).
    """
    rows = db.session.execute(text("""
        SELECT
          c.meter_id,
          m.meter_number                 AS meter_label,
          LOWER(m.utility_type)          AS utility_type,
          c.last_date                    AS prev_date,
          c.last_read                    AS prev_value,
          c.new_date                     AS curr_date,
          c.new_read                     AS curr_value,
          c.days                         AS days,
          c.consumption                  AS consumption
        FROM bil_consumption c
        JOIN bil_meter m ON m.id = c.meter_id
        ORDER BY CASE WHEN LOWER(m.utility_type)='electricity' THEN 0 ELSE 1 END,
                 m.meter_number
    """)).mappings().all()

    out = []
    for r in rows:
        out.append({
            "meter_id":    int(r["meter_id"]),
            "meter_label": r["meter_label"],
            "utility":     (r["utility_type"] or "").lower(),
            "prev_date":   r["prev_date"],
            "prev_value":  int(r["prev_value"] or 0),
            "curr_date":   r["curr_date"],
            "curr_value":  int(r["curr_value"] or 0),
            "days":        int(r["days"] or 0),
            "cons":        int(r["consumption"] or 0),  # INT consumption
        })
    return out


# ───────────────────────────────────────────────────────────────────────────────
# Page builders
# ───────────────────────────────────────────────────────────────────────────────

def build_metsoa_rows(_tenant_id_unused, _month_unused):
    """
    Page 1: copy consumption; add Elec due via flat rate; for Water add
    three summary lines per meter using tiers + charge map.
    """
    cons = _consumption_rows_from_table()
    elec = [r for r in cons if r["utility"] == "electricity"]
    wtrs = [r for r in cons if r["utility"] == "water"]

    # Electricity (flat)
    elec_rows = []
    elec_total = 0.0
    elec_rate = _latest_electric_rate()

    for r in elec:
        due = int(r["cons"]) * elec_rate
        elec_total += due
        elec_rows.append({
            "meter_label": r["meter_label"],
            "prev_date":   r["prev_date"],
            "prev_value":  r["prev_value"],
            "curr_date":   r["curr_date"],
            "curr_value":  r["curr_value"],
            "days":        r["days"],
            "consumption": r["cons"],
            "rate":        elec_rate,
            "due":         round(due, 2),
            "is_summary":  False,
        })
    elec_total = round(elec_total, 2)

    # Water
    water_rows = []
    water_total = 0.0
    water_tiers = _latest_tiers("water")
    sanit_tiers = _latest_tiers("sanitation")

    for r in wtrs:
        cons_kl = float(r["cons"] or 0)

        # base line (no rate/due)
        water_rows.append({
            "meter_label": r["meter_label"],
            "prev_date":   r["prev_date"],
            "prev_value":  r["prev_value"],
            "curr_date":   r["curr_date"],
            "curr_value":  r["curr_value"],
            "days":        r["days"],
            "consumption": r["cons"],
            "rate":        None,
            "due":         None,
            "is_summary":  False,
        })

        # tiered charges
        ws_tier_due, _ = _apply_tiers(cons_kl, water_tiers, apply_reduction=False)
        sd_tier_due, _ = _apply_tiers(cons_kl, sanit_tiers, apply_reduction=True)

        # mapped extras
        ws_extra, sd_extra, _ = _mapped_extras_for_meter(int(r["meter_id"]), cons_kl)

        ws_total = ws_tier_due + ws_extra
        sd_total = sd_tier_due + sd_extra
        meter_total = ws_total + sd_total
        water_total += meter_total

        # three summary rows
        water_rows += [
            { "meter_label": f"W&S Cost for #{r['meter_label']}",
              "prev_date": None, "prev_value": None, "curr_date": None, "curr_value": None,
              "days": None, "consumption": None,
              "rate": round(ws_total, 2), "due": None, "is_summary": True },
            { "meter_label": f"S & D Cost for #{r['meter_label']}",
              "prev_date": None, "prev_value": None, "curr_date": None, "curr_value": None,
              "days": None, "consumption": None,
              "rate": round(sd_total, 2), "due": None, "is_summary": True },
            { "meter_label": f"Water Cost for #{r['meter_label']}",
              "prev_date": None, "prev_value": None, "curr_date": None, "curr_value": None,
              "days": None, "consumption": None,
              "rate": None, "due": round(meter_total, 2), "is_summary": True },
        ]

    water_total = round(water_total, 2)
    due_to_metro = round(elec_total + water_total, 2)

    return elec_rows, elec_total, water_rows, water_total, due_to_metro


def build_water_details(_tenant_id_unused, _month_unused):
    """
    Page 2: detailed water tiers + per-meter mapped extras.
    """
    cons = _consumption_rows_from_table()
    water_tiers = _latest_tiers("water")
    sanit_tiers = _latest_tiers("sanitation")

    out = []
    for r in cons:
        if r["utility"] != "water":
            continue

        cons_kl = float(r["cons"] or 0)
        ws_due, ws_lines = _apply_tiers(cons_kl, water_tiers, apply_reduction=False)
        sd_due, sd_lines = _apply_tiers(cons_kl, sanit_tiers, apply_reduction=True)
        ws_extra, sd_extra, extra_lines = _mapped_extras_for_meter(int(r["meter_id"]), cons_kl)

        ws_total = round(ws_due + ws_extra, 2)
        sd_total = round(sd_due + sd_extra, 2)
        meter_total = round(ws_total + sd_total, 2)

        out.append({
            "meter_label": r["meter_label"],
            "cons": int(r["cons"]),
            "ws_lines": ws_lines,
            "sd_lines": sd_lines,
            "extra_lines": extra_lines,
            "ws_total": ws_total,
            "sd_total": sd_total,
            "meter_total": meter_total,
        })
    return out

##
# ---------- date helpers ----------
def _month_bounds(month_str):
    # month_str = "YYYY-MM"
    y, m = map(int, month_str.split("-"))
    from datetime import date
    from calendar import monthrange
    first = date(y, m, 1)
    # next month
    if m == 12:
        nxt = date(y + 1, 1, 1)
    else:
        nxt = date(y, m + 1, 1)
    last = date(y, m, monthrange(y, m)[1])
    return first, last, nxt

# ---------- core lookups ----------
def _fetch_consumption_rows(tenant_id, month_str):
    """
    Load the rows for Page 1 & Page 2 from bil_consumption.
    1) First try the schema that has c.tenant_id.
    2) If that column doesn't exist, fall back to MONTH-ONLY (no tenant filter).
       This matches your flow where the consumption run is already per-tenant/month.
    """
    # 1) Try path with c.tenant_id
    try:
        rows = db.session.execute(text("""
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
            WHERE c.tenant_id = :tid AND c.month = :mon
            ORDER BY CASE WHEN lower(m.utility_type)='electricity' THEN 0 ELSE 1 END,
                     m.meter_number
        """), {"tid": tenant_id, "mon": month_str}).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        pass

    # 2) Fallback: month-only (NO reference to m.tenant_id)
    rows = db.session.execute(text("""
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
        WHERE c.month = :mon
        ORDER BY CASE WHEN lower(m.utility_type)='electricity' THEN 0 ELSE 1 END,
                 m.meter_number
    """), {"mon": month_str}).mappings().all()
    return [dict(r) for r in rows]

def _elec_rate_for_month(month_str):
    first, _, _ = _month_bounds(month_str)
    row = db.session.execute(text("""
        SELECT rate
        FROM bil_tariff
        WHERE utility_type='electricity' AND date(effective_date) <= :d
        ORDER BY date(effective_date) DESC
        LIMIT 1
    """), {"d": first}).fetchone()
    return float(row[0]) if row else 0.0

def _tariffs_for(utility_type, month_str):
    first, _, _ = _month_bounds(month_str)
    rows = db.session.execute(text("""
        SELECT code, rate, block_start, block_end, COALESCE(reduction_factor, NULL) AS rf
        FROM bil_tariff
        WHERE utility_type = :ut AND date(effective_date) <= :d
        ORDER BY block_start ASC, block_end ASC
    """), {"ut": utility_type, "d": first}).mappings().all()
    out = []
    for r in rows:
        out.append({
            "code": r["code"],
            "rate": float(r["rate"]),
            "start": float(r["block_start"] or 0.0),  # liters per day
            "end": float(r["block_end"] or 0.0),
            "rf": None if r["rf"] is None else float(r["rf"]),
        })
    return out

def _charge_map_for_meter(meter_id, month_str):
    first, _, _ = _month_bounds(month_str)
    rows = db.session.execute(text("""
        SELECT charge_code, utility_type
        FROM bil_meter_charge_map
        WHERE meter_id=:mid
          AND is_enabled=1
          AND date(effective_start) <= :d
    """), {"mid": meter_id, "d": first}).mappings().all()
    # return a set of codes for quick membership checks
    return { (r["charge_code"], (r["utility_type"] or "").lower()) for r in rows }

# ---------- tier engines ----------
def _alloc_kl_by_tier(cons_kl, days, tiers):
    """
    tiers: list with .start and .end in L/day, .rate per kL
    Convert daily bands to monthly kL capacity and allocate.
    Returns list of (code, used_kl, rate, rf)
    """
    if not cons_kl or cons_kl <= 0:
        return [(t["code"], 0.0, t["rate"], t["rf"]) for t in tiers]

    left = float(cons_kl)
    out = []
    for t in tiers:
        # band size (kL) for the period
        daily_from = (t["start"] or 0.0) / 1000.0
        daily_to   = (t["end"]   or 0.0) / 1000.0
        if daily_to <= 0:  # open-ended last tier
            cap = max(left, 0.0)
        else:
            cap = max((daily_to - daily_from) * float(days), 0.0)

        used = min(left, cap)
        out.append((t["code"], used, t["rate"], t["rf"]))
        left -= used
        if left <= 1e-9:
            # still need to append remaining tiers with 0 to keep shape stable
            pass
    # If consumption exceeded provided caps and last tier has finite end,
    # treat overflow at last tier’s rate:
    if left > 1e-9 and tiers:
        last = tiers[-1]
        out[-1] = (last["code"], out[-1][1] + left, last["rate"], last["rf"])
    return out

def _sum_ws(cons_kl, days, month_str):
    tiers = _tariffs_for("water", month_str)
    alloc = _alloc_kl_by_tier(cons_kl, days, tiers)
    total = sum(used * rate for _, used, rate, _ in alloc)
    # Also return the per-line detail for page-2
    lines = [{"label": code.replace("_", " "), "cons": used, "rate": rate, "due": used * rate}
             for code, used, rate, _ in alloc if used > 0]
    return total, lines

def _sum_sd(cons_kl, days, month_str):
    tiers = _tariffs_for("sanitation", month_str)
    alloc = _alloc_kl_by_tier(cons_kl, days, tiers)
    # reduction_factor applies to the *chargeable* volume
    lines = []
    total = 0.0
    # Fallback reductions if rf missing (municipal default)
    rf_defaults = {
        "Tier1_SD": 0.95,
        "Tier2_SD": 0.75,
        "Tier3_SD": 0.75,
        "Tier4_SD": 0.65,
    }
    for code, used, rate, rf in alloc:
        if used <= 0:
            continue
        rfac = rf if rf is not None else rf_defaults.get(code, 1.0)
        chargeable = used * rfac
        due = chargeable * rate
        lines.append({
            "label": code.replace("_", " "),
            "cons": chargeable,  # show the reduced kL
            "rate": rate,
            "due": due,
        })
        total += due
    return total, lines

# ---------- page-1 builder ----------
def build_metsoa_rows(tenant_id, month_str):
    rows = _fetch_consumption_rows(tenant_id, month_str)
    elec_rate = _elec_rate_for_month(month_str)

    elec_rows, elec_total = [], 0.0
    water_rows, water_total = [], 0.0

    for r in rows:
        util = (r["utility_type"] or "").lower()
        cons = int(r["consumption"] or 0)
        days = int(r["days"] or 0)

        if util == "electricity":
            due = cons * elec_rate
            elec_rows.append({
                "meter_label": r["meter_label"], "prev_date": r["prev_date"], "prev_value": r["prev_value"],
                "curr_date": r["curr_date"], "curr_value": r["curr_value"], "days": days,
                "consumption": cons, "rate": elec_rate, "due": due
            })
            elec_total += due
            continue

        if util == "water":
            meter_id = r["meter_id"]
            mlabel   = r["meter_label"]
            # start with the meter line (blank rate/due, exactly like your UI)
            water_rows.append({
                "kind": "meter",
                "meter_label": mlabel, "prev_date": r["prev_date"], "prev_value": r["prev_value"],
                "curr_date": r["curr_date"], "curr_value": r["curr_value"], "days": days,
                "consumption": cons, "rate": None, "due": None
            })

            # gated charges via the map
            map_codes = _charge_map_for_meter(meter_id, month_str)

            # Tiers (variable)
            ws_total, _ = _sum_ws(cons, days, month_str) if ("Tier1_W&S", "water") or ("water", "water") else (0.0, [])
            sd_total, _ = _sum_sd(cons, days, month_str)

            # Per-kL surcharge (only if mapped). If cons = 0 → 0
            ws_surcharge = 0.0
            if ("WSSurcharge", "water") in map_codes or ("WSSurcharge", "") in map_codes:
                row = db.session.execute(text("""
                    SELECT rate FROM bil_tariff
                    WHERE code='WSSurcharge'
                    ORDER BY date(effective_date) DESC LIMIT 1
                """)).fetchone()
                if row:
                    ws_surcharge = cons * float(row[0])

            sd_surcharge = 0.0
            if ("SDSurcharge", "sanitation") in map_codes or ("SDSurcharge", "") in map_codes:
                row = db.session.execute(text("""
                    SELECT rate FROM bil_tariff
                    WHERE code='SDSurcharge'
                    ORDER BY date(effective_date) DESC LIMIT 1
                """)).fetchone()
                if row:
                    sd_surcharge = cons * float(row[0])

            # Fixed charges (only if mapped)
            wll = 0.0
            if ("WaterLossLevy", "water") in map_codes or ("WaterLossLevy", "") in map_codes:
                row = db.session.execute(text("""
                    SELECT rate FROM bil_tariff WHERE code='WaterLossLevy'
                    ORDER BY date(effective_date) DESC LIMIT 1
                """)).fetchone()
                if row:
                    wll = float(row[0])

            refuse = 0.0
            if ("RefuseBin", "sanitation") in map_codes or ("RefuseBin", "") in map_codes:
                row = db.session.execute(text("""
                    SELECT rate FROM bil_tariff WHERE code='RefuseBin'
                    ORDER BY date(effective_date) DESC LIMIT 1
                """)).fetchone()
                if row:
                    refuse = float(row[0])

            mgmt = 0.0
            if ("MgmtFee", "management") in map_codes or ("MgmtFee", "") in map_codes:
                row = db.session.execute(text("""
                    SELECT rate FROM bil_tariff WHERE code='MgmtFee'
                    ORDER BY date(effective_date) DESC LIMIT 1
                """)).fetchone()
                if row:
                    mgmt = float(row[0])

            ws_total_full = ws_total + ws_surcharge + wll + mgmt
            sd_total_full = sd_total + sd_surcharge + refuse
            meter_water_total = ws_total_full + sd_total_full

            # Page-1 formatting: show WS & SD amounts in Rate column; final water cost in Due
            water_rows.append({"kind": "ws", "label": f"W&S Cost for {mlabel}", "rate": ws_total_full, "due": None})
            water_rows.append({"kind": "sd", "label": f"S & D Cost for {mlabel}", "rate": sd_total_full, "due": None})
            water_rows.append({"kind": "sum", "label": f"Water Cost for {mlabel}", "rate": None, "due": meter_water_total})

            water_total += meter_water_total

    due_total = elec_total + water_total
    return elec_rows, elec_total, water_rows, water_total, due_total

#####
# utils/billing_helpers.py


# --- small helpers -----------------------------------------------------------



# --- data access -------------------------------------------------------------




# --- core math ---------------------------------------------------------------








# --- page builders -----------------------------------------------------------



# utils/billing_helpers.py  (or wherever build_metsoa_rows() reads the base)
def _fetch_base_rows_for_month(month_str: str):
    rows = db.session.execute(text("""
        SELECT
          c.meter_id,
          m.meter_number                AS meter_label,
          lower(trim(m.utility_type))   AS utility_type,   -- normalize here
          c.last_date                   AS prev_date,
          c.last_read                   AS prev_value,
          c.new_date                    AS curr_date,
          c.new_read                    AS curr_value,
          c.days                        AS days,
          c.consumption                 AS consumption
        FROM bil_consumption c
        JOIN bil_meter m ON m.id = c.meter_id
        WHERE c.month = :month
        ORDER BY CASE WHEN lower(trim(m.utility_type))='electricity' THEN 0 ELSE 1 END,
                 m.meter_number
    """), {"month": month_str}).mappings().all()  # mappings() → dict-like access
    return rows

def _is_water_by_map(meter_id: int, month_str: str) -> bool:
    """Return True if this meter has any WATER/SANITATION charges active in the map for the month."""
    first_of_month = f"{month_str}-01"
    row = db.session.execute(text("""
        SELECT 1
        FROM bil_meter_charge_map
        WHERE meter_id = :mid
          AND is_enabled = 1
          AND (effective_start IS NULL OR effective_start <= :d)
          AND (effective_end   IS NULL OR effective_end   >= :d)
          AND lower(utility_type) IN ('water','sanitation')
        LIMIT 1
    """), {"mid": meter_id, "d": first_of_month}).fetchone()
    return row is not None

######
def _rank_charge_code(code: str) -> int:
    c = (code or "").strip()
    if c == "ElecRate":      return 10

    # Water & Sanitation (WS group first, then SD group, then fixed add-ons)
    if c == "WS_Tiered":     return 100
    if c == "WSSurcharge":   return 110
    if c == "WaterLossLevy": return 120

    if c == "SD_Tiered":     return 200
    if c == "SDSurcharge":   return 210

    if c == "RefuseBin":     return 220
    if c == "MgmtFee":       return 900

    # Unknown / future codes
    return 800


def _month_end(month_str: str) -> str:
    # month_str = 'YYYY-MM' -> month_end 'YYYY-MM-31' (string compare OK with ISO dates)
    # You can be exact with last-day if you want; for filtering purposes 31 works.
    return f"{month_str}-31"


def _latest_tariff_row(code: str, utility: str, month_str: str):
    """
    Returns a single mapping row from bil_tariff for (utility, code)
    with effective_date <= month_end, most recent first.
    Fields expected: rate, reduction_factor (nullable), block_start, block_end, unit
    """
    end_key = _month_end(month_str)
    row = db.session.execute(text("""
        SELECT id, utility_type, code, rate,
               COALESCE(reduction_factor, 1.0) AS reduction_factor,
               block_start, block_end, unit, effective_date
        FROM bil_tariff
        WHERE utility_type = :ut
          AND code = :code
          AND effective_date <= :end_key
        ORDER BY effective_date DESC
        LIMIT 1
    """), {"ut": utility, "code": code, "end_key": end_key}).mappings().first()
    return row


def _tier_tariffs_for_utility(utility: str, month_str: str):
    """
    Collect tiered tariff rows (ordered by block_end asc) for 'water' or 'sanitation'.
    Expects rows for codes like Tier1_W&S ... or Tier1_SD ...
    """
    end_key = _month_end(month_str)
    rows = db.session.execute(text("""
        SELECT code, rate,
               COALESCE(reduction_factor, 1.0) AS reduction_factor,
               block_start, block_end, unit
        FROM bil_tariff
        WHERE utility_type = :ut
          AND code LIKE 'Tier%_%'
          AND effective_date <= :end_key
        ORDER BY block_end ASC
    """), {"ut": utility, "end_key": end_key}).mappings().all()
    return rows

def _map_rows_for_meter(meter_id: int, month_str: str, utility_type: str):
    """
    Pull map rows for this meter (enabled & within date window), rank them,
    and fall back to sensible defaults if none exist.
    """
    end_key = _month_end(month_str)

    rows = db.session.execute(text("""
        SELECT id, meter_id, charge_code, utility_type,
               COALESCE(tariff_code_override, charge_code) AS effective_code
        FROM bil_meter_charge_map
        WHERE meter_id = :mid
          AND is_enabled = 1
          AND (effective_start IS NULL OR effective_start <= :end_key)
          AND (effective_end   IS NULL OR effective_end   >= :end_key)
    """), {"mid": meter_id, "end_key": end_key}).mappings().all()

    if not rows:
        # Fallback: electric → ElecRate; water → full WS/SD pack
        util = (utility_type or "").strip().lower()
        if util.startswith("elec"):
            rows = [{"charge_code": "ElecRate", "utility_type": "electricity", "effective_code": "ElecRate"}]
        else:
            rows = [
                {"charge_code": "WS_Tiered",     "utility_type": "water",      "effective_code": "WS_Tiered"},
                {"charge_code": "WSSurcharge",   "utility_type": "water",      "effective_code": "WSSurcharge"},
                {"charge_code": "WaterLossLevy", "utility_type": "water",      "effective_code": "WaterLossLevy"},
                {"charge_code": "SD_Tiered",     "utility_type": "sanitation", "effective_code": "SD_Tiered"},
                {"charge_code": "SDSurcharge",   "utility_type": "sanitation", "effective_code": "SDSurcharge"},
                {"charge_code": "RefuseBin",     "utility_type": "sanitation", "effective_code": "RefuseBin"},
            ]

    # Rank and return
    ranked = []
    for r in rows:
        cc = r["effective_code"]
        ranked.append({
            **r,
            "rank": _rank_charge_code(cc),
        })
    ranked.sort(key=lambda x: (x["rank"], x["effective_code"]))
    return ranked

def _alloc_tiers(cons_kl: float, days: int, tier_rows):
    """
    Allocate 'cons_kl' into progressive tiers where each tier cap is:
       ((block_end - block_start) liters/day) * days / 1000
    Returns (total_amount, breakdown_list)
      breakdown_list items: { "code", "cons_kl", "rate", "amount" }
    """
    days = max(1, int(days or 0))
    remaining = float(cons_kl or 0.0)
    total = 0.0
    breakdown = []

    prev_end_lday = 0.0
    for tr in tier_rows:
        # liters/day bounds → per-period kL cap for THIS tier (incremental)
        bs = float(tr.get("block_start") or 0.0)   # liters/day
        be = float(tr.get("block_end")   or 0.0)   # liters/day
        if be <= prev_end_lday:
            continue
        span_lday = be - max(prev_end_lday, bs)    # liters/day in this tier
        cap_kl = (span_lday * days) / 1000.0       # kL allowance for this tier
        take = min(remaining, max(0.0, cap_kl))
        if take > 0:
            rate = float(tr.get("rate") or 0.0)
            amt = round(take * rate, 2)
            total += amt
            breakdown.append({
                "code": tr.get("code"),
                "cons_kl": round(take, 3),
                "rate": rate,
                "amount": amt,
            })
            remaining -= take
        prev_end_lday = be
        if remaining <= 0.000001:
            break

    # If consumption exceeds highest tier cap, treat excess at last tier rate.
    if remaining > 0 and tier_rows:
        last = tier_rows[-1]
        rate = float(last.get("rate") or 0.0)
        amt = round(remaining * rate, 2)
        total += amt
        breakdown.append({
            "code": f"{last.get('code')}+",
            "cons_kl": round(remaining, 3),
            "rate": rate,
            "amount": amt,
        })

    return round(total, 2), breakdown


def get_electricity_rate_for_month(month_str: str):
    t = _latest_tariff_row("ElecRate", "electricity", month_str)
    return float((t or {}).get("rate") or 0.0)


def build_metsoa_rows(tenant_id: int, month_str: str):
    """
    Builds:
      elec_rows:   list of base electricity rows (with rate & due)
      elec_total:  float
      water_rows:  list where each water base row is followed by 3 lines:
                   WS (amount in Rate col), SD (amount in Rate col),
                   Water Cost (WS+SD in Due col? -> per your last spec, Due shows pure water cost)
      water_total: float (sum of all (WS + SD + Water Cost) or just WS+SD? You asked
                           Page 1 to display WS in Rate, SD in Rate, and Water Cost in Due; we total
                           “metro due” as Elec + WS + SD + Water Cost)
      due_to_metro: elec_total + sum(all three per water meter)
    """
    base_rows = get_consumption_rows_for_month(tenant_id, month_str)

    elec_rows, water_rows = [], []
    elec_total = 0.0
    water_total = 0.0

    elec_rate = get_electricity_rate_for_month(month_str)

    for r in base_rows:
        util = (r["utility_type"] or "").strip().lower()
        cons = int(r["consumption"] or 0)
        days = int(r["days"] or 0)

        if util.startswith("elec"):
            due = round(cons * (elec_rate or 0.0), 2)
            elec_rows.append({
                "meter":       r["meter_label"],
                "prev_date":   r["prev_date"],
                "prev_value":  r["prev_value"],
                "curr_date":   r["curr_date"],
                "curr_value":  r["curr_value"],
                "days":        r["days"],
                "consumption": cons,
                "rate":        elec_rate,
                "due":         due,
            })
            elec_total += due or 0.0

        else:
            # Base water row (Rate/Due blank)
            water_rows.append({
                "kind":        "water-cons",
                "meter":       r["meter_label"],
                "prev_date":   r["prev_date"],
                "prev_value":  r["prev_value"],
                "curr_date":   r["curr_date"],
                "curr_value":  r["curr_value"],
                "days":        r["days"],
                "consumption": cons,
                "rate":        None,
                "due":         None,
            })

            totals = calc_ws_sd_totals(
                meter_id=r["meter_id"],
                month_str=month_str,
                consumption_kl=cons,
                days=days,
                #utility_type_hint=r["utility_type"],
            ) or {"ws_amount": 0.0, "sd_amount": 0.0, "water_cost": 0.0}

            ws_amt = totals.get("ws_amount") or 0.0
            sd_amt = totals.get("sd_amount") or 0.0
            w_amt  = totals.get("water_cost") or 0.0

            # Two “Rate” lines (WS, SD), then one “Due” line (Water Cost)
            water_rows.append({"kind": "ws",          "label": f"W&S Cost for # {r['meter_label']}",  "rate": ws_amt})
            water_rows.append({"kind": "sd",          "label": f"S & D Cost # {r['meter_label']}",    "rate": sd_amt})
            water_rows.append({"kind": "water-total", "label": f"Water Cost for # {r['meter_label']}", "due":  w_amt})

            # Total ‘due to metro’ includes all three components for water meters
            #water_total += (ws_amt + sd_amt + w_amt)
            water_total += (w_amt or 0.0)
    due_to_metro = round((elec_total or 0.0) + (water_total or 0.0), 2)
    return elec_rows, round(elec_total, 2), water_rows, round(water_total, 2), due_to_metro

def get_base_rows_from_copy():
    """
    Returns the staged rows (already filtered by your data-prep step)
    as dict-like mappings for METSOA Page 1.

    Columns returned:
      meter_id, meter_label, utility_type,
      prev_date, prev_value, curr_date, curr_value, days, consumption
    """
    q = """
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
        FROM bil_consumption_copy c
        JOIN bil_meter m ON m.id = c.meter_id
        ORDER BY
          CASE WHEN lower(m.utility_type) LIKE 'elec%' THEN 0 ELSE 1 END,
          m.meter_number
    """
    return db.session.execute(text(q)).mappings().all()

def _tariff_map():
    """
    Returns { code: {rate: float, reduction_factor: float|None} } from bil_tariff.
    """
    rows = db.session.execute(text("""
        SELECT code, rate, COALESCE(reduction_factor, 0) AS rf
        FROM bil_tariff
    """)).mappings().all()
    return {r["code"]: {"rate": float(r["rate"]), "rf": float(r["rf"] or 0)} for r in rows}




ZERO_WS_SD_TOTALS = {"ws_amount": 0.0, "sd_amount": 0.0, "water_cost": 0.0, "ws_lines": [], "sd_lines": []}

def build_metsoa_page2_sections(tenant_id: int, month_str: str):
    """
    One section per water meter.
    Each section has: header (meter + base readings), ws_lines, sd_lines, ws_total, sd_total.
    """
    sections = []
    base_rows = get_consumption_rows_for_month(tenant_id, month_str)

    for r in base_rows:
        util = (r["utility_type"] or "").strip().lower()
        if not util.startswith("w"):  # only water-family meters
            continue

        cons = int(r["consumption"] or 0)
        days = int(r["days"] or 0)

        totals = calc_ws_sd_totals(
            meter_id=r["meter_id"],
            month_str=month_str,
            consumption_kl=cons,
            days=days,
        ) or ZERO_WS_SD_TOTALS

        ws_lines = totals.get("ws_lines", [])
        sd_lines = totals.get("sd_lines", [])
        ws_total = round(sum(x.get("due", 0.0) for x in ws_lines), 2)
        sd_total = round(sum(x.get("due", 0.0) for x in sd_lines), 2)

        sections.append({
            "meter":        r["meter_label"],     # ← ensures “Meter None” never happens
            "prev_date":    r["prev_date"],
            "prev_value":   r["prev_value"],
            "curr_date":    r["curr_date"],
            "curr_value":   r["curr_value"],
            "days":         days,
            "consumption":  cons,
            "ws_lines":     ws_lines,
            "ws_total":     ws_total,
            "sd_lines":     sd_lines,
            "sd_total":     sd_total,
        })

    return sections

# utils/billing_helpers.py

# helpers/billing_water.py
from decimal import Decimal, ROUND_HALF_UP, getcontext
from sqlalchemy import text
from flask import current_app as app


# Money-safe context
getcontext().prec = 28
Q2 = Decimal("0.01")
D31 = Decimal("31")

def _D(x) -> Decimal:
    try:
        return Decimal(str(x or 0))
    except Exception:
        return Decimal("0")

def _q2(x: Decimal) -> Decimal:
    return x.quantize(Q2, rounding=ROUND_HALF_UP)

def calc_ws_sd_totals(
    meter_id: int,
    month_str: str,
    consumption_kl: float | int,
    days: int | None = None,
    include_fixed: bool = True,
    want_breakdown: bool = False,
):
    """
    Computes Water & Sewer (Sanitation) for a meter/month with:
      • Money-safe Decimal arithmetic (rounds HALF_UP to 2dp)
      • Scaled tier caps by actual 'days' (31-day model)
      • Deterministic ordering (order field)
      • Backwards-compatible shape: returns ws_lines/sd_lines & ws_total/sd_total

    Returns dict:
      {
        "ws_total": Decimal(2dp),
        "sd_total": Decimal(2dp),
        "water_cost": Decimal(2dp),
        "ws_lines": [ {desc, qty, rate, due, order, style?}, ... ],
        "sd_lines": [ ... ]
      }
    """
    d_days = _D(days or 31)
    cons = _D(consumption_kl)

    # ---------- tariff lookup with tiny in-call cache ----------
    _cache: dict[str, tuple[Decimal, Decimal]] = {}

    def _tariff(code: str) -> tuple[Decimal, Decimal]:
        """Return (rate, reduction_factor). If code missing, (0,1)."""
        if not code:
            return Decimal("0"), Decimal("1")
        if code in _cache:
            return _cache[code]
        row = db.session.execute(
            text("""
                SELECT rate, COALESCE(reduction_factor,1.0) AS rf
                FROM bil_tariff
                WHERE code = :c
                ORDER BY date(effective_date) DESC
                LIMIT 1
            """),
            {"c": code},
        ).mappings().first()
        rate = _D(row["rate"]) if row else Decimal("0")
        rf   = _D(row["rf"])   if row else Decimal("1")
        _cache[code] = (rate, rf)
        return rate, rf

    def _add_line(lst, desc: str, qty: Decimal | None, rate: Decimal | None, due: Decimal, order: int, style: str | None = None):
        lst.append({
            "desc": desc,
            "qty": (None if qty is None else _q2(qty)),
            "rate": (None if rate is None else _q2(rate)),
            "due":  _q2(due),
            "order": order,
            **({"style": style} if style else {}),
        })

    # ---------- tier model (kL per 31 days; scaled by actual days) ----------
    def _cap(kl31: str) -> Decimal:
        return ( _D(kl31) * (d_days / D31) ).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)

    # Labels match the style in your sample
    TIERS = [
        # (label, cap_kl_per_31days, tariff_code, order)
        ("0L-200L/31Days",    "0.200", "W_T1", 10),
        ("201L-833L/31Days",  "0.833", "W_T2", 20),
        ("833L-1KL/31Days",   "1.000", "W_T3", 30),
        ("1KL-1,5KL/31Days",  "1.500", "W_T4", 40),
    ]

    ws_lines: list[dict] = []
    sd_lines: list[dict] = []

    # ---------- WATER: split across tiers ----------
    remaining = cons
    prev_cap = Decimal("0")
    for label, cap_31, code, order in TIERS:
        cap = _cap(cap_31)
        band = max(Decimal("0"), min(remaining, cap - prev_cap))
        if band > 0:
            rate, rf = _tariff(code)
            due = band * rate * rf
            _add_line(ws_lines, label, band, rate, due, order)
            remaining -= band
        prev_cap = cap

    # Anything above top cap? Bill it at the last tier rate if present
    if remaining > 0:
        rate, rf = _tariff("W_T4")
        due = remaining * rate * rf
        _add_line(ws_lines, "Above 1,5KL (pro-rated)", remaining, rate, due, 50)

    # ---------- WATER EXTRAS (auto: per-kL if *_PERKL exists, else fixed if plain code exists) ----------
    def _optional_water(name: str, base_order: int, qty_for_perkl: Decimal):
        rate, rf = _tariff(f"{name}_PERKL")
        if rate > 0:
            due = qty_for_perkl * rate * rf
            _add_line(ws_lines, name.replace("_", " ").title(), qty_for_perkl, rate, due, base_order)
            return
        rate, rf = _tariff(name)
        if rate > 0:
            due = rate * rf
            _add_line(ws_lines, name.replace("_", " ").title(), None, rate, due, base_order)

    _optional_water("W_SUR",   90, cons)   # Surcharge
    _optional_water("W_LOSS",  95, cons)   # Water Loss Levy
    if include_fixed:
        rate, rf = _tariff("W_MGMT")       # Monthly Management Fee (fixed)
        if rate > 0:
            due = rate * rf
            _add_line(ws_lines, "Monthly Management Fee", None, rate, due, 98)

    # ---------- SEWER side ----------
    # Strategy: if there are tiered sewer codes (S_T1..), use them; else fallback to fixed items.
    sd_remaining = cons
    used_sewer_tiers = False
    for code, label, order in [("S_T1","0L-200L/31Days",10), ("S_T2","201L-833L/31Days",20), ("S_T3","833L-1KL/31Days",30), ("S_T4","1KL-1,5KL/31Days",40)]:
        rate, rf = _tariff(code)
        if rate <= 0:
            continue
        used_sewer_tiers = True
        cap = _cap(TIERS[(order//10)-1][1])  # reuse water caps in same positions
        band = max(Decimal("0"), min(sd_remaining, cap - (Decimal("0") if order == 10 else _cap(TIERS[(order//10)-2][1]))))
        if band > 0:
            due = band * rate * rf
            _add_line(sd_lines, label, band, rate, due, order)
            sd_remaining -= band
    if used_sewer_tiers and sd_remaining > 0:
        rate, rf = _tariff("S_T4")
        if rate > 0:
            due = sd_remaining * rate * rf
            _add_line(sd_lines, "Above 1,5KL (pro-rated)", sd_remaining, rate, due, 50)

    # Sewer extras (auto per-kL or fixed)
    def _optional_sewer(name: str, base_order: int, qty_for_perkl: Decimal):
        rate, rf = _tariff(f"{name}_PERKL")
        if rate > 0:
            due = qty_for_perkl * rate * rf
            _add_line(sd_lines, name.replace("_", " ").title(), qty_for_perkl, rate, due, base_order)
            return
        rate, rf = _tariff(name)
        if rate > 0:
            due = rate * rf
            _add_line(sd_lines, name.replace("_", " ").title(), None, rate, due, base_order)

    _optional_sewer("S_SUR",   90, cons)     # Surcharge
    if include_fixed:
        _optional_sewer("S_REFUSE", 96, cons)  # Refuse Bins (usually fixed)

    # ---------- totals ----------
    ws_total = _q2(sum((ln["due"] for ln in ws_lines), Decimal("0")))
    sd_total = _q2(sum((ln["due"] for ln in sd_lines), Decimal("0")))

    if want_breakdown:
        # dashed separator + explicit subtotal rows so template renders nicely
        if ws_lines:
            _add_line(ws_lines, "", None, None, ws_total, 9990, style="separator")
            _add_line(ws_lines, f"W&S Cost For #{meter_id}", None, None, ws_total, 9991, style="subtotal")
        if sd_lines:
            _add_line(sd_lines, "", None, None, sd_total, 9990, style="separator")
            _add_line(sd_lines, f"S & D Cost #{meter_id}", None, None, sd_total, 9991, style="subtotal")

    return {
        "ws_total": ws_total,
        "sd_total": sd_total,
        "water_cost": _q2(ws_total + sd_total),
        "ws_lines": ws_lines if want_breakdown else [],
        "sd_lines": sd_lines if want_breakdown else [],
    }

    # ---- helpers -------------------------------------------------------------
    def _tariff(code: str):
        row = db.session.execute(text("""
            SELECT rate, COALESCE(reduction_factor,1.0) AS rf
            FROM bil_tariff
            WHERE code = :c
            ORDER BY date(effective_date) DESC
            LIMIT 1
        """), {"c": code}).mappings().first()
        if not row:
            return 0.0, 1.0
        return float(row["rate"] or 0.0), float(row["rf"] or 1.0)

    def _chunk(cons: float, low: float, high: float) -> float:
        # amount of cons that falls in [low, high)
        if cons <= low:
            return 0.0
        return max(0.0, min(cons, high) - low)

    # ---- inputs --------------------------------------------------------------
    cons = float(int(consumption_kl or 0))  # force integer kL
    ws_lines, sd_lines = [], []

    # Map rows for this meter (enabled only). Dates are already normalized.
    map_rows = db.session.execute(text("""
        SELECT charge_code, utility_type, COALESCE(is_enabled,1) AS is_enabled
        FROM bil_meter_charge_map
        WHERE meter_id = :mid AND COALESCE(is_enabled,1) = 1
        ORDER BY
          CASE charge_code
            WHEN 'WSSurcharge'   THEN 1
            WHEN 'SDSurcharge'   THEN 2
            WHEN 'WaterLossLevy' THEN 3
            WHEN 'RefuseBin'     THEN 4
            WHEN 'MgmtFee'       THEN 5
            WHEN 'WS_Tiered'     THEN 10
            WHEN 'SD_Tiered'     THEN 11
            ELSE 99
          END
    """), {"mid": meter_id}).mappings().all()

    # Quick lookup so we can ask "does this meter have X?"
    has = {r["charge_code"]: True for r in map_rows}

    ws_total = 0.0
    sd_total = 0.0

    # ---- tiered WATER (W&S) --------------------------------------------------
    if has.get("WS_Tiered"):
        tiers = [
            (0.0,   200.0,  "Tier1_W&S"),
            (200.0, 833.0,  "Tier2_W&S"),
            (833.0, 1000.0, "Tier3_W&S"),
            (1000.0,1500.0, "Tier4_W&S"),
        ]
        for low, high, code in tiers:
            vol = _chunk(cons, low, high)
            if vol <= 0:
                continue
            rate, _rf = _tariff(code)
            due = round(vol * rate, 2)
            ws_total += due
            if want_breakdown:
                ws_lines.append({"line": code, "cons": vol, "rate": rate, "due": due})

    # ---- tiered SANITATION (reduced %) --------------------------------------
    if has.get("SD_Tiered"):
        tiers_sd = [
            (0.0,   200.0,  "Tier1_SD"),
            (200.0, 833.0,  "Tier2_SD"),
            (833.0, 1000.0, "Tier3_SD"),
            (1000.0,1500.0, "Tier4_SD"),
        ]
        for low, high, code in tiers_sd:
            vol = _chunk(cons, low, high)
            if vol <= 0:
                continue
            rate, rf = _tariff(code)  # rf is the reduction fraction (e.g., 0.95, 0.75, 0.65)
            red_vol = vol * rf
            due = round(red_vol * rate, 2)
            sd_total += due
            if want_breakdown:
                sd_lines.append({"line": f"{code} @ {rf:.2f}", "cons": red_vol, "rate": rate, "due": due})

    # ---- fixed/linear extras -------------------------------------------------
    if include_fixed:
        # Surcharges scale with consumption (0 → 0)
        if has.get("WSSurcharge"):
            rate, _ = _tariff("WSSurcharge")
            due = round(cons * rate, 2)
            ws_total += due
            if want_breakdown:
                ws_lines.append({"line": "Surcharge", "cons": cons, "rate": rate, "due": due})

        if has.get("SDSurcharge"):
            rate, _ = _tariff("SDSurcharge")
            due = round(cons * rate, 2)
            sd_total += due
            if want_breakdown:
                sd_lines.append({"line": "Surcharge", "cons": cons, "rate": rate, "due": due})

        # Fixed monthly items
        if has.get("WaterLossLevy"):
            rate, _ = _tariff("WaterLossLevy")
            due = round(rate, 2)
            ws_total += due
            if want_breakdown:
                ws_lines.append({"line": "Water Loss Levy", "cons": 0, "rate": rate, "due": due})

        if has.get("MgmtFee"):
            rate, _ = _tariff("MgmtFee")
            due = round(rate, 2)
            ws_total += due
            if want_breakdown:
                ws_lines.append({"line": "Monthly Management Fee", "cons": 0, "rate": rate, "due": due})

        if has.get("RefuseBin"):
            rate, _ = _tariff("RefuseBin")
            due = round(rate, 2)
            sd_total += due
            if want_breakdown:
                sd_lines.append({"line": "Refuse Bins", "cons": 0, "rate": rate, "due": due})

    result = {
        "ws_amount": round(ws_total, 2),
        "sd_amount": round(sd_total, 2),
        "water_cost": round(ws_total + sd_total, 2),
    }
    if want_breakdown:
        result["ws_lines"] = ws_lines
        result["sd_lines"] = sd_lines
    return result

# app/admin/billing/helpers_recurring.py

#from app.models import BilTenantLedger, BilRecurringItem  # adjust import paths

def _first_valid_post_date(year: int, month: int, day_of_month: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    dom = min(max(1, day_of_month), last_day)  # clamp
    return date(year, month, dom)
'''
def ensure_recurring_materialized(tenant_id: int, period_month: str):
    """
    Ensure all active recurring items for tenant are posted into the ledger for period_month (YYYY-MM).
    Rent will be locked and non-editable.
    """
    # Parse period
    y, m = [int(x) for x in period_month.split("-")]

    # Load active recurring items that fall within the month
    recurs = (
        db.session.query(BilRecurringItem)
        .filter(
            BilRecurringItem.tenant_id == tenant_id,
            BilRecurringItem.is_active == 1,
        )
        .all()
    )

    for r in recurs:
        # Check if r applies to this month
        start_ym = tuple(map(int, r.start_month.split("-")))
        end_ym = tuple(map(int, r.end_month.split("-"))) if r.end_month else None
        this_ym = (y, m)

        if this_ym < start_ym:
            continue
        if end_ym and this_ym > end_ym:
            continue

        # Compose a unique source key
        source_key = f"recurring:{(r.name or 'item').lower()}:{r.id}:{period_month}"

        # Already posted?
        existing = (
            db.session.query(BilTenantLedger)
            .filter(
                BilTenantLedger.tenant_id == tenant_id,
                BilTenantLedger.period_month == period_month,
                BilTenantLedger.source_key == source_key,
            )
            .first()
        )
        if existing:
            continue

        # Compute posting date in this month
        post_date = _first_valid_post_date(y, m, r.day_of_month)

        # Insert locked ledger row
        row = BilTenantLedger(
            tenant_id=tenant_id,
            item_date=post_date,
            description=r.name,
            kind=r.kind,                        # 'charge'
            charge_amount=r.amount if r.kind == 'charge' else None,
            payment_amount=r.amount if r.kind == 'credit' else None,
            ref=r.source_code,
            period_month=period_month,
            source="recurring",
            source_key=source_key,
            locked=1,                           # <- cannot be edited
        )
        db.session.add(row)

    db.session.commit()
'''
def materialize_recurring_for_month_sql(tenant_id: int, month_ym: str):
    """
    Ensure recurring rows are inserted into bil_tenant_ledger for this month.
    No ORM, only SQL.
    """
    from datetime import date
    import calendar

    y, m = [int(x) for x in month_ym.split("-")]
    start = date(y, m, 1)
    last_day = calendar.monthrange(y, m)[1]
    end = date(y, m, last_day)

    recurs = db.session.execute(
        text("""
            SELECT id, description, kind, amount, day_of_month
            FROM bil_tenant_recurring
            WHERE tenant_id=:tid AND is_active=1
        """),
        {"tid": tenant_id},
    ).mappings().all()

    for r in recurs:
        auto_ref = f"AUTO:REC:{r['id']}:{month_ym}"

        exists = db.session.execute(
            text("""
                SELECT id FROM bil_tenant_ledger
                WHERE tenant_id=:tid AND ref=:ref
                  AND date(txn_date) BETWEEN date(:start) AND date(:end)
                LIMIT 1
            """),
            {"tid": tenant_id, "ref": auto_ref,
             "start": start.isoformat(), "end": end.isoformat()},
        ).first()
        if exists:
            continue

        dom = min(max(1, r["day_of_month"]), last_day)
        post_date = date(y, m, dom).isoformat()
        amt = float(r["amount"] or 0.0)
        signed_amt = amt if r["kind"] == "charge" else -amt

        db.session.execute(
            text("""
                INSERT INTO bil_tenant_ledger
                    (tenant_id, txn_date, description, kind, ref, amount)
                VALUES (:tid, :txn_date, :desc, :kind, :ref, :amount)
            """),
            {
                "tid": tenant_id,
                "txn_date": post_date,
                "desc": r["description"],
                "kind": r["kind"],
                "ref": auto_ref,
                "amount": signed_amt,
            },
        )

    db.session.commit()

##
# --- begin: SQL-only recurring materializer (no ORM models) ---



def _bh_month_bounds(ym: str):
    """Return (start_date_str, end_date_str) for 'YYYY-MM'."""
    y, m = [int(x) for x in ym.split("-")]
    start = date(y, m, 1).isoformat()
    end = date(y, m, calendar.monthrange(y, m)[1]).isoformat()
    return start, end

def _bh_clamp_day(y: int, m: int, d: int) -> str:
    """Clamp day-of-month into the month and return ISO date."""
    last = calendar.monthrange(y, m)[1]
    dom = min(max(1, int(d or 1)), last)
    return date(y, m, dom).isoformat()

def ensure_recurring_materialized(tenant_id: int, period_month: str) -> None:
    """
    Pure-SQL.
    Ensures each ACTIVE recurring item is posted exactly once into bil_tenant_ledger
    for the given month (YYYY-MM). Uses deterministic ref 'AUTO:REC:<rec_id>:YYYY-MM'
    to prevent duplicates. Charges are +amount; credits/payments are -amount.
    """
    y, m = [int(x) for x in period_month.split("-")]
    month_start, month_end = _bh_month_bounds(period_month)

    # Load active recurring item definitions (Rent etc.)
    recurs = db.session.execute(
        text("""
            SELECT
              id,
              description,           -- e.g., 'Rent'
              kind,                  -- 'charge' or 'credit'/'payment'
              amount,
              day_of_month,
              is_active,
              COALESCE(start_month, '') AS start_month, -- optional 'YYYY-MM'
              COALESCE(end_month,   '') AS end_month    -- optional 'YYYY-MM'
            FROM bil_tenant_recurring
            WHERE tenant_id = :tid AND is_active = 1
        """),
        {"tid": tenant_id},
    ).mappings().all()

    for r in recurs:
        # Honor optional start/end month windows
        this_ym = (y, m)
        if r["start_month"]:
            sy, sm = [int(x) for x in r["start_month"].split("-")]
            if this_ym < (sy, sm):
                continue
        if r["end_month"]:
            ey, em = [int(x) for x in r["end_month"].split("-")]
            if this_ym > (ey, em):
                continue

        rec_id   = r["id"]
        desc     = r["description"]
        kind     = r["kind"]                      # 'charge' or 'credit'/'payment'
        amt      = float(r["amount"] or 0.0)
        post_dt  = _bh_clamp_day(y, m, r["day_of_month"])
        auto_ref = f"AUTO:REC:{rec_id}:{period_month}"

        # Already posted for this month?
        exists = db.session.execute(
            text("""
                SELECT id
                FROM bil_tenant_ledger
                WHERE tenant_id = :tid
                  AND ref = :ref
                  AND date(txn_date) BETWEEN date(:start_d) AND date(:end_d)
                LIMIT 1
            """),
            {
                "tid": tenant_id, "ref": auto_ref,
                "start_d": month_start, "end_d": month_end
            },
        ).first()
        if exists:
            continue

        # Signed amount: charges positive; credits/payments negative
        signed_amt = amt if kind == "charge" else -abs(amt)

        # Insert actual ledger row
        db.session.execute(
            text("""
                INSERT INTO bil_tenant_ledger
                    (tenant_id, txn_date, description, kind, ref, amount)
                VALUES
                    (:tid, :txn_date, :desc, :kind, :ref, :amount)
            """),
            {
                "tid": tenant_id,
                "txn_date": post_dt,
                "desc": desc,
                "kind": kind,
                "ref": auto_ref,  # marker used by UI to show "Locked"
                "amount": signed_amt,
            },
        )

    db.session.commit()

# --- end: SQL-only recurring materializer ---

