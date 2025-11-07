# admin/routes_readings.py

from flask import (
    Blueprint, render_template_string, send_file, current_app, render_template, 
    request, redirect, session, url_for, flash, 
    Response, abort, 
    make_response)
from app.extensions import db
from app.models.billing import (
    BilLease, BilMeterFixedCharge, BilTenant, BilMeter, BilMeterReading,
    BilSectionalUnit)
from datetime import datetime, date, timedelta
from sqlalchemy import and_, func, select, text, or_
from app.auth.forms import LoginForm
from app.admin.billing.water import (
    get_consumption_rows_for_month,_month_bounds,
    calc_ws_sd_for_meter,calc_ws_sd_totals, SAN_TIER_CODES, WATER_TIER_CODES,
     recompute_and_upsert_water_totals, _tariff_latest_by_code, 
     upsert_meter_month_total, upsert_tenant_month_water_totals)
#from app.utils.billing_helpers import build_metsoa_rows,  get_consumption_rows_for_month
from app.utils.billing_map import compute_electricity_due
from app.utils.billing_metsoa import build_metsoa_page2_groups
from app.utils.billing_metsoa_builder import build_metsoa_payload
from app.utils.billing_persist import commit_metsoa_for_month
from .. import admin_bp
import calendar
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
import io, csv, zipfile, re
from decimal import Decimal, InvalidOperation
from app.admin.billing.electric import upsert_electricity_line  # <- the helper above
from sqlalchemy.orm import joinedload
from io import BytesIO
#from weasyprint import HTML


@admin_bp.route("/billing/tenants", endpoint="billing_tenants")
def billing_tenants():
    return render_template("admin/billing/tenants.html")

@admin_bp.route("/billing/units", endpoint="billing_units")
def billing_units():
    return render_template("admin/billing/units.html")

@admin_bp.route("/billing/meters", endpoint="billing_meters")
def billing_meters():
    return render_template("admin/billing/meters.html")

@admin_bp.route("/billing/tariffs", endpoint="billing_tariffs")
def billing_tariffs():
    return render_template("admin/billing/tariffs.html")

@admin_bp.route("/billing/invoices", endpoint="billing_invoices")
def billing_invoices():
    return render_template("admin/billing/invoices.html")

@admin_bp.route("/billing/reports", endpoint="billing_reports")
def billing_reports():
    return render_template("admin/billing/reports.html")

# Step 1: Enter
@admin_bp.route('/readings/create', methods=['POST'])
def readings_create():
    meter_id = int(request.form['meter_id'])
    reading_date = datetime.strptime(request.form['reading_date'], '%Y-%m-%d').date()
    reading_value = float(request.form['reading_value'])

    dup = (BilMeterReading.query
           .filter_by(meter_id=meter_id, reading_date=reading_date)
           .first())
    if dup:
        dup.reading_value = reading_value
        action = 'updated'
    else:
        db.session.add(BilMeterReading(
            meter_id=meter_id,
            reading_date=reading_date,
            reading_value=reading_value
        ))
        action = 'saved'
    db.session.commit()
    flash(f'Reading {action}.', 'success')
    return redirect(url_for('admin_bp.billing_readings_dashboard'))

# Step 2: View
# app/admin/billing/routes.py  (add/replace this view)


@admin_bp.route("/readings/view", methods=["GET"], endpoint="readings_view")
def readings_view():
    tenants = BilTenant.query.order_by(BilTenant.name).all()

    # Distinct months present in readings, newest first (SQLite strftime)
    month_rows = (db.session.query(func.strftime('%Y-%m', BilMeterReading.reading_date))
                  .distinct()
                  .order_by(func.strftime('%Y-%m', BilMeterReading.reading_date).desc())
                  .all())
    months = [m[0] for m in month_rows]

    tenant_id = request.args.get("tenant_id", type=int)
    month = request.args.get("month")  # "YYYY-MM"

    rows = []
    tenant = BilTenant.query.get(tenant_id) if tenant_id else None

    if tenant and month:
        y, m = map(int, month.split("-"))
        first = date(y, m, 1)
        next_first = date(y + (m == 12), (m % 12) + 1, 1)
        last = next_first - timedelta(days=1)

        q = (db.session.query(BilMeter, BilMeterReading)
             .join(BilMeterReading, BilMeterReading.meter_id == BilMeter.id)
             .filter(BilMeterReading.reading_date >= first,
                     BilMeterReading.reading_date <= last))

        # Filter meters by tenant. If your BilMeter has tenant_id, use that; else use sectional_unit_id.
        if hasattr(BilMeter, "tenant_id"):
            q = q.filter(BilMeter.tenant_id == tenant.id)
        else:
            # assumes BilTenant has sectional_unit_id and BilMeter.sectional_unit_id exists
            q = q.filter(BilMeter.sectional_unit_id == tenant.sectional_unit_id)

        rows = q.order_by(BilMeter.id, BilMeterReading.reading_date).all()

    return render_template(
        "admin/billing/readings_view.html",
        tenants=tenants,
        months=months,
        tenant=tenant,
        selected_tenant_id=tenant_id,
        selected_month=month,
        rows=rows,
        breadcrumbs=[
            ("Meter Reading", url_for("admin_bp.readings_dashboard")),
            ("Enter", None),
        ],
        nav_back_url=url_for("admin_bp.readings_dashboard"),
    )

# Step 3: Compute Consumption

# Step 4: Export / Print
@admin_bp.route('/readings/export', methods=['GET'])
def readings_export():
    q = BilMeterReading.query.join(BilMeter)
    meter_id = request.args.get('meter_id') or None
    from_date = request.args.get('from_date') or None
    to_date   = request.args.get('to_date') or None
    formats = request.args.getlist('format')  # e.g. ['csv', 'print']

    if meter_id:
        q = q.filter(BilMeterReading.meter_id == int(meter_id))
    if from_date:
        q = q.filter(BilMeterReading.reading_date >= from_date)
    if to_date:
        q = q.filter(BilMeterReading.reading_date <= to_date)

    rows = q.order_by(BilMeterReading.meter_id, BilMeterReading.reading_date).all()

    if 'csv' in formats:
        import csv, io
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(['meter_id','meter_name','reading_date','reading_value'])
        for r in rows:
            w.writerow([r.meter_id, r.meter.display_name or f"Meter #{r.meter_id}",
                        r.reading_date.isoformat(), r.reading_value])
        return Response(buf.getvalue(),
                        mimetype='text/csv',
                        headers={'Content-Disposition': 'attachment; filename=readings.csv'})

    return render_template("aadmin_bp/billing/readings_export.html", rows=rows)

# --- Overview: just 4 buttons ---
@admin_bp.route("/readings", methods=["GET"], endpoint="readings_dashboard")
def readings_dashboard():
    return render_template("admin/billing/index.html")

# Helpers ---------------------------------------------------------------

def _month_bounds(d: date):
    first_this = d.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    return last_prev.replace(day=1), last_prev

def _prev_month_reading(meter_id: int, reading_dt: date):
    first_prev, last_prev = _month_bounds(reading_dt)
    return (
        BilMeterReading.query
        .filter(
            BilMeterReading.meter_id == meter_id,
            BilMeterReading.reading_date >= first_prev,
            BilMeterReading.reading_date <= last_prev,
        )
        .order_by(BilMeterReading.reading_date.desc())
        .first()
    )
# Route ----------------------------------------------------------------

@admin_bp.route("/readings/enter", methods=["GET", "POST"], endpoint="readings_enter")
def readings_enter():
    tenants = BilTenant.query.order_by(BilTenant.name).all()
    tenant_id = request.values.get("tenant_id", type=int)
    tenant = BilTenant.query.get(tenant_id) if tenant_id else None

    meters = []
    if tenant:
        if hasattr(BilMeter, "tenant_id"):
            meters = BilMeter.query.filter_by(tenant_id=tenant.id).order_by(BilMeter.id).all()
        elif hasattr(BilMeter, "sectional_unit_id") and hasattr(tenant, "sectional_unit_id"):
            meters = BilMeter.query.filter_by(sectional_unit_id=tenant.sectional_unit_id).order_by(BilMeter.id).all()

    warning_same, prev_info = None, None

    if request.method == "POST":
        meter_id = request.form.get("meter_id", type=int)
        reading_date = datetime.strptime(request.form["reading_date"], "%Y-%m-%d").date()
        reading_value = int(request.form["reading_value"])   # ← integers only

        # 1) SAME-DAY record handling first
        existing_same_day = BilMeterReading.query.filter_by(meter_id=meter_id, reading_date=reading_date).first()
        if existing_same_day:
            old = int(existing_same_day.reading_value)
            if reading_value < old:
                flash(f"Incorrect reading: {reading_value} is BELOW existing value {old} on {reading_date}. Not accepted.", "error")
                return render_template("admin/billing/readings_enter.html",
                                       tenants=tenants, tenant=tenant, meters=meters)
            # if equal or higher, we allow update below

        # 2) Compare to PREVIOUS MONTH latest
        prev = _prev_month_reading(meter_id, reading_date)
        if prev:
            prev_val = int(prev.reading_value)
            prev_info = prev
            if reading_value < prev_val:
                flash(f"Incorrect reading: {reading_value} is BELOW previous month {prev_val} ({prev.reading_date}). Not accepted.", "error")
                return render_template("admin/billing/readings_enter.html",
                                       tenants=tenants, tenant=tenant, meters=meters, prev_info=prev_info)

        # 3) Upsert by (meter_id, date)
        if existing_same_day:
            existing_same_day.reading_value = reading_value
            msg = "Reading updated."
        else:
            db.session.add(BilMeterReading(meter_id=meter_id, reading_date=reading_date, reading_value=reading_value))
            msg = "Reading saved."
        db.session.commit()
        flash(msg, "success")
        return redirect(url_for("admin_bp.readings_enter", tenant_id=tenant_id))
    
    return render_template(
        "admin/billing/readings_enter.html",
        tenants=tenants,
        tenant=tenant,
        meters=meters,
        warning_same=locals().get("warning_same"),
        prev_info=locals().get("prev_info"),
        breadcrumbs=[
            ("Meter Reading", url_for("admin_bp.readings_dashboard")),
            ("Enter", None),
        ],
        # optional if your layout shows a header Back button:
        # nav_back_url=url_for("admin_bp.readings_dashboard"),
    )


@admin_bp.route("/readings/consumption", methods=["GET"], endpoint="readings_consumption")
def readings_consumption():
    # Dropdown data
    tenants = BilTenant.query.order_by(BilTenant.name).all()
    # Build YYYY-MM list from existing readings without needing sqlalchemy.func
    month_rows = db.session.query(BilMeterReading.reading_date).distinct().all()
    months = sorted({d[0].strftime("%Y-%m") for d in month_rows if d[0]}, reverse=True)

    tenant_id = request.args.get("tenant_id", type=int)
    month = request.args.get("month")  # "YYYY-MM"

    rows = []
    tenant = BilTenant.query.get(tenant_id) if tenant_id else None

    def _month_range(ym):
        y, m = map(int, ym.split("-"))
        first = date(y, m, 1)
        # next month first
        next_first = date(y + (m == 12), (m % 12) + 1, 1)
        last = next_first - timedelta(days=1)
        return first, last

    def _latest_in_range(meter_id, start_dt, end_dt):
        return (BilMeterReading.query
                .filter(BilMeterReading.meter_id == meter_id,
                        BilMeterReading.reading_date >= start_dt,
                        BilMeterReading.reading_date <= end_dt)
                .order_by(BilMeterReading.reading_date.desc())
                .first())

    if tenant and month:
        curr_first, curr_last = _month_range(month)
        # previous month
        prev_last_day = curr_first - timedelta(days=1)
        prev_first = prev_last_day.replace(day=1)

        # Meters for tenant (supports tenant_id OR sectional_unit_id schema)
        if hasattr(BilMeter, "tenant_id"):
            meters = (BilMeter.query
                      .filter_by(tenant_id=tenant.id)
                      .order_by(BilMeter.id).all())
        elif hasattr(BilMeter, "sectional_unit_id") and hasattr(tenant, "sectional_unit_id"):
            meters = (BilMeter.query
                      .filter_by(sectional_unit_id=tenant.sectional_unit_id)
                      .order_by(BilMeter.id).all())
        else:
            meters = []

        for m in meters:
            curr = _latest_in_range(m.id, curr_first, curr_last)
            prev = _latest_in_range(m.id, prev_first, prev_last_day)

            if curr and prev:
                cons = int(curr.reading_value) - int(prev.reading_value)
                days = (curr.reading_date - prev.reading_date).days
            else:
                cons = None
                days = None

            rows.append({
                "meter_label": (m.meter_number or getattr(m, "display_name", None) or f"Meter #{m.id}"),
                "prev_date": prev.reading_date if prev else None,
                "prev_value": int(prev.reading_value) if prev else None,
                "curr_date": curr.reading_date if curr else None,
                "curr_value": int(curr.reading_value) if curr else None,
                "days": days,
                "consumption": cons,
            })

    return render_template(
    "admin/billing/readings_consumption.html",
    tenants=tenants,
    months=months,
    tenant=tenant,
    selected_tenant_id=tenant_id,
    selected_month=month,
    rows=rows,
    breadcrumbs=[
        ("Meter Reading", url_for("admin_bp.readings_dashboard")),
        ("Enter", None),
        ],
    nav_back_url=url_for("admin_bp.readings_dashboard"),
    )

# Billing home (unchanged if you already have this)
@admin_bp.route("/billing/", endpoint="billing_home")
def billing_home():
    return render_template("admin/billing/index.html")

@admin_bp.route("/billing/metsoa/water/<int:meter_id>", methods=["GET", "POST"], endpoint="metsoa_water_config")
def metsoa_water_config(meter_id):
    tenant_id = request.args.get("tenant_id", type=int)
    month = request.args.get("month")
    if not tenant_id or not month:
        flash("Missing tenant or month.", "error")
        return redirect(url_for("admin_bp.readings_consumption"))

    tenant = BilTenant.query.get(tenant_id)
    meter = BilMeter.query.get(meter_id)
    if not tenant or not meter:
        flash("Tenant or meter not found.", "error")
        return redirect(url_for("admin_bp.readings_consumption"))

    # --- helpers (local) ---
    def _month_range(ym: str):
        y, m = map(int, ym.split("-"))
        first = date(y, m, 1)
        next_first = date(y + (m == 12), (m % 12) + 1, 1)
        last = next_first - timedelta(days=1)
        return first, last

    def _latest_in_range(mid: int, start_dt: date, end_dt: date):
        return (BilMeterReading.query
                .filter(BilMeterReading.meter_id == mid,
                        BilMeterReading.reading_date >= start_dt,
                        BilMeterReading.reading_date <= end_dt)
                .order_by(BilMeterReading.reading_date.desc())
                .first())

    def _get_fixed(desc: str):
        return (BilMeterFixedCharge.query
                .filter_by(meter_id=meter_id, month=month, description=desc)
                .first())

    def _upsert_fixed(desc: str, utility_type: str, amount: float, rate: float = None, cons: float = None):
        row = _get_fixed(desc)
        if row is None:
            row = BilMeterFixedCharge(
                meter_id=meter_id,
                month=month,
                description=desc,
                utility_type=utility_type,
            )
            db.session.add(row)
        row.amount = float(amount) if amount is not None else None
        row.rate = float(rate) if rate is not None else None
        row.cons = float(cons) if cons is not None else None
        return row

    # --- dates & readings ---
    curr_first, curr_last = _month_range(month)
    prev_last = curr_first - timedelta(days=1)
    prev_first = prev_last.replace(day=1)

    prev = _latest_in_range(meter_id, prev_first, prev_last)
    curr = _latest_in_range(meter_id, curr_first, curr_last)

    consumption = None
    days = None
    if curr and prev:
        consumption = int(curr.reading_value) - int(prev.reading_value)
        days = (curr.reading_date - prev.reading_date).days

    # Preload existing values for edit
    ws_row = _get_fixed("WS Cost")
    sd_row = _get_fixed("S & D Cost")
    water_row = _get_fixed("Water Cost")

    if request.method == "POST":
        # WATER: either flat or computed elsewhere — for first pass we accept a simple amount
        water_rate = request.form.get("water_rate", type=float)
        water_due = request.form.get("water_due", type=float)
        # If water rate is given and we have consumption, derive due
        if (water_due is None or water_due == 0) and (water_rate is not None and consumption is not None):
            water_due = round(water_rate * consumption, 2)

        # WS components (free-form to match your sample lines)
        ws_base_due      = request.form.get("ws_base_due", type=float)       # "0L-200L/26Days"
        ws_surcharge_due = request.form.get("ws_surcharge_due", type=float)  # "Surcharge"
        ws_loss_due      = request.form.get("ws_loss_due", type=float)       # "Water Loss Levy"
        ws_mgmt_due      = request.form.get("ws_mgmt_due", type=float)       # "Monthly Management Fee"
        ws_total = sum(x for x in [ws_base_due, ws_surcharge_due, ws_loss_due, ws_mgmt_due] if x)

        # SD components
        sd_base_due      = request.form.get("sd_base_due", type=float)       # "0L-200L/26Days"
        sd_surcharge_due = request.form.get("sd_surcharge_due", type=float)  # "Surcharge"
        sd_refuse_due    = request.form.get("sd_refuse_due", type=float)     # "Refuse Bins"
        sd_total = sum(x for x in [sd_base_due, sd_surcharge_due, sd_refuse_due] if x)

        # Persist into BilMeterFixedCharge (3 rows)
        _upsert_fixed("WS Cost",   "water_services", ws_total)
        _upsert_fixed("S & D Cost","sanitation",     sd_total)
        _upsert_fixed("Water Cost","water",          water_due, rate=water_rate, cons=consumption)

        db.session.commit()
        flash("Water charges saved.", "success")
        return redirect(url_for("admin_bp.metsoa_page1", tenant_id=tenant.id, month=month))

    return render_template(
        "admin/billing/metsoa_water_config.html",
        tenant=tenant,
        month=month,
        meter=meter,
        prev=prev,
        curr=curr,
        consumption=consumption,
        days=days,
        # prefill existing
        ws_total=ws_row.amount if ws_row else None,
        sd_total=sd_row.amount if sd_row else None,
        water_rate=water_row.rate if water_row else None,
        water_due=water_row.amount if water_row else None,
    )


@admin_bp.route("/billing/metsoa/build")
def metsoa_build():
    tenant_id = request.args.get("tenant_id", type=int)
    month = request.args.get("month", type=str)
    if not tenant_id or not month:
        abort(400)

    # Build everything in one pass and store once
    page1, page2 = build_metsoa_payload(tenant_id, month)
    session["metsoa_page1"] = page1
    session["metsoa_page2"] = page2
    session["metsoa_context"] = {"tenant_id": tenant_id, "month": month}

    return redirect(url_for("admin_bp.metsoa_page1", tenant_id=tenant_id, month=month))

def _get_tenant_month_water_totals(tenant_id: int, month: str):
    # Prefer aggregated table; otherwise sum per-meter table.
    row = db.session.execute(text("""
        SELECT ws_total, sd_total, water_total
        FROM bil_metsoa_tenant_month
        WHERE tenant_id = :t AND month = :mon
    """), {"t": tenant_id, "mon": month}).mappings().first()
    if row:
        return float(row["ws_total"] or 0), float(row["sd_total"] or 0), float(row["water_total"] or 0)

    row = db.session.execute(text("""
        SELECT
          COALESCE(SUM(ws_total),0)    AS ws_total,
          COALESCE(SUM(sd_total),0)    AS sd_total,
          COALESCE(SUM(water_cost),0)  AS water_total
        FROM bil_metsoa_meter_month
        WHERE tenant_id = :t AND month = :mon
    """), {"t": tenant_id, "mon": month}).mappings().first()
    return float(row["ws_total"]), float(row["sd_total"]), float(row["water_total"])

# app/admin/billing/routes.py



@admin_bp.route("/admin/billing/metsoa")
def metsoa_page1():
    tenant_id = request.args.get("tenant_id", type=int)
    month     = request.args.get("month", type=str)
    if not tenant_id or not month: abort(400)
    month = datetime.strptime(month, "%Y-%m").strftime("%Y-%m")

    tenant = db.session.get(BilTenant, tenant_id) or abort(404)

    base_rows = get_consumption_rows_for_month(tenant_id, month) or []
    elec_sources  = [r for r in base_rows if (r.get("utility_type") or "").lower().startswith("e")]
    water_sources = [r for r in base_rows if (r.get("utility_type") or "").lower().startswith("w")]

    # ---- Electricity table ----
    # Get the latest ElecRate; fall back to 0 if missing.
    e_tar = _tariff_latest_by_code("ElecRate")
    elec_rate = float(e_tar["rate"]) if e_tar else 0.0

    elec_rows = []
    elec_total = 0.0
    for r in elec_sources:
        prev_date = r.get("prev_date"); curr_date = r.get("curr_date")
        prev_read = r.get("prev_value") or r.get("prev_read")
        curr_read = r.get("curr_value") or r.get("curr_read")
        days      = int(r.get("days") or 0)
        cons      = float(r.get("consumption") or r.get("cons") or 0)
        due       = round(cons * elec_rate, 2)

        upsert_electricity_line(
            tenant_id=tenant_id, meter_id=r["meter_id"], month=month,
            prev_date=prev_date, prev_read=prev_read,
            curr_date=curr_date, curr_read=curr_read,
            days=days, consumption=cons,
            rate=elec_rate, due=due
        )

        elec_rows.append({
            "meter": r.get("meter_label") or str(r["meter_id"]),
            "prev_date": prev_date, "prev_read": prev_read,
            "curr_date": curr_date, "curr_read": curr_read,
            "days": days, "cons": cons, "rate": elec_rate, "due": due,
        })
        elec_total += due

    # ---- Water table (one line per water meter) ----
    water_rows = []
    ws_total_all = 0.0
    sd_total_all = 0.0
    water_total_all = 0.0

    for r in water_sources:
        prev_date = r.get("prev_date"); curr_date = r.get("curr_date")
        prev_read = r.get("prev_value") or r.get("prev_read")
        curr_read = r.get("curr_value") or r.get("curr_read")
        days      = int(r.get("days") or 0)
        cons      = int(r.get("consumption") or r.get("cons") or 0)

        t = calc_ws_sd_totals(
            meter_id=r["meter_id"], month_str=month,
            consumption_kl=cons, days=days,
            include_fixed=True, want_breakdown=False
        )

        # Persist water per meter (keeps Page 2 in sync)
        upsert_meter_month_total(
            tenant_id=tenant_id, meter_id=r["meter_id"], month=month,
            ws=float(t["ws_total"]), sd=float(t["sd_total"]), wc=float(t["water_cost"]),
        )

        water_rows.append({
            "meter":      r.get("meter_label") or str(r["meter_id"]),
            "prev_date":  prev_date, "prev_read": prev_read,
            "curr_date":  curr_date, "curr_read": curr_read,
            "days": days, "cons": cons,
            "rate": None,                         # tiered -> show dash in UI
            "due": float(t["water_cost"]),        # WS + SD per meter
        })

        ws_total_all    += float(t["ws_total"])
        sd_total_all    += float(t["sd_total"])
        water_total_all += float(t["water_cost"])

    # Aggregate once for the tenant-month
    upsert_tenant_month_water_totals(
        tenant_id=tenant_id, month=month,
        ws=ws_total_all, sd=sd_total_all, wc=water_total_all
    )

    # ---- Totals to show in the footer ----
    water_total = round(water_total_all, 2)
    elec_total  = round(elec_total, 2)

    # Hook up refuse/rates/other when available
    refuse_total = 0.0
    rates_total  = 0.0
    other_total  = 0.0

    due_to_metro = round(water_total + elec_total + refuse_total + rates_total, 2)
    grand_total  = round(due_to_metro + other_total, 2)

    return render_template(
        "admin/billing/metsoa_page1.html",
        tenant=tenant, month=month,
        # Tables
        elec_rows=elec_rows, water_rows=water_rows,
        # Footer lines
        elec_total=elec_total, water_total=water_total,
        refuse_total=refuse_total, rates_total=rates_total, other_total=other_total,
        due_to_metro=due_to_metro, grand_total=grand_total,
    )

def _upsert_meter_month_total(tenant_id: int, meter_id: int, month: str, ws: float, sd: float, wc: float):
    # SQLite/PG-friendly UPSERT; if your table/index differs, adjust the ON CONFLICT target accordingly.
    try:
        db.session.execute(text("""
            INSERT INTO bil_metsoa_meter_month
                (tenant_id, meter_id, month, ws_total, sd_total, water_cost, updated_at)
            VALUES (:t, :m, :mon, :ws, :sd, :wc, CURRENT_TIMESTAMP)
            ON CONFLICT(tenant_id, meter_id, month)
            DO UPDATE SET
                ws_total  = excluded.ws_total,
                sd_total  = excluded.sd_total,
                water_cost= excluded.water_cost,
                updated_at= CURRENT_TIMESTAMP
        """), {"t": tenant_id, "m": meter_id, "mon": month, "ws": ws, "sd": sd, "wc": wc})
        db.session.commit()
    except Exception:
        db.session.rollback()  # non-fatal if table/constraint not present

@admin_bp.route("/billing/metsoa/page2")
def metsoa_page2():
    tenant_id = request.args.get("tenant_id", type=int)
    month     = request.args.get("month", type=str)
    if not tenant_id or not month:
        abort(400)

    tenant = db.session.get(BilTenant, tenant_id) or abort(404)
    base_rows   = get_consumption_rows_for_month(tenant_id, month) or []
    water_bases = [r for r in base_rows if (r.get("utility_type") or "").strip().lower().startswith("w")]

    sections = []

    # NEW: accumulate across all meters
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

        # Persist for Page 1 (unchanged)
        _upsert_meter_month_total(
            tenant_id=tenant_id,
            meter_id=r["meter_id"],
            month=month,
            ws=float(totals["ws_total"]),
            sd=float(totals["sd_total"]),
            wc=float(totals["water_cost"]),
        )

        # NEW: accumulate page-level totals
        page_ws_total   += float(totals["ws_total"])
        page_sd_total   += float(totals["sd_total"])
        page_water_total += float(totals["water_cost"])

        # Pass per-meter totals to template (for small line under each card)
        sections.append({
            "meter":      r.get("meter_label") or str(r["meter_id"]),
            "prev_date":  r.get("prev_date"),
            "prev_value": r.get("prev_value"),
            "curr_date":  r.get("curr_date"),
            "curr_value": r.get("curr_value"),
            "days":       days,
            "cons":       cons,
            "ws_lines":   totals["ws_lines"],
            "sd_lines":   totals["sd_lines"],
            "ws_total":   float(totals["ws_total"]),     # NEW
            "sd_total":   float(totals["sd_total"]),     # NEW
            "water_cost": float(totals["water_cost"]),   # NEW
        })

    return render_template(
        "admin/billing/metsoa_page2.html",
        tenant=tenant,
        month=month,
        sections=sections,
        page_ws_total=round(page_ws_total, 2),       # NEW
        page_sd_total=round(page_sd_total, 2),       # NEW
        water_total=round(page_water_total, 2),      # Cost of Water (WS+SD)
    )

def _upsert_tenant_month_water_totals(tenant_id: int, month: str, ws: float, sd: float, wc: float):
    try:
        db.session.execute(text("""
            INSERT INTO bil_metsoa_tenant_month
                (tenant_id, month, ws_total, sd_total, water_total, updated_at)
            VALUES (:t, :mon, :ws, :sd, :wc, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, month)
            DO UPDATE SET
                ws_total   = excluded.ws_total,
                sd_total   = excluded.sd_total,
                water_total= excluded.water_total,
                updated_at = CURRENT_TIMESTAMP
        """), {"t": tenant_id, "mon": month, "ws": ws, "sd": sd, "wc": wc})
        db.session.commit()
    except Exception:
        db.session.rollback()  # safe if table doesn't exist

@admin_bp.route("/billing/metsoa/commit", methods=["GET","POST"])
def metsoa_commit():
    tenant_id = request.values.get("tenant_id", type=int)
    month     = request.values.get("month", type=str)
    if not tenant_id or not month:
        abort(400)

    # recompute & upsert — keep your existing logic
    try:
        from app.utils.billing_helpers import build_metsoa_rows
        _, elec_total, _, water_total, due_to_metro = build_metsoa_rows(tenant_id, month)
        amt = round(float(due_to_metro or 0.0), 2)
        db.session.execute(text("""
            INSERT INTO bil_tenant_ledger
              (tenant_id, month, description, kind, amount, debit, credit, txn_date, created_at)
            VALUES
              (:tid, :mon, 'Due to Metro', 'charge', :amt, :amt, 0, date(:mon || '-01'), datetime('now'))
            ON CONFLICT(tenant_id, month, kind, description)
            DO UPDATE SET amount=excluded.amount, debit=excluded.debit, credit=excluded.credit, txn_date=excluded.txn_date
        """), {"tid": tenant_id, "mon": month, "amt": amt})
        db.session.commit()
        flash(f"Posted to ledger for {month}: {amt:.2f}", "success")
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Commit to ledger failed")
        flash(f"Commit error: {getattr(e, 'orig', e)}", "danger")
        return redirect(f"/admin/admin/billing/metsoa?tenant_id={tenant_id}&month={month}")

    # 🔒 Force landing on the ledger page you actually use in your app right now
    return redirect(f"/admin/admin/billing/ledger?tenant_id={tenant_id}&month={month}")

@admin_bp.route("/billing/tenant/soa")
def tenant_soa():
    month     = request.values.get("month", type=str)  # 'YYYY-MM'
    tenant_id = request.args.get("tenant_id", type=int)
    if not tenant_id:
        abort(400)
    start = request.args.get("from")  # 'YYYY-MM-DD' optional
    end   = request.args.get("to")    # 'YYYY-MM-DD' optional

    tenant = db.session.get(BilTenant, tenant_id)

    # Pull ledger rows
    if start and end:
        rows = db.session.execute(text("""
          SELECT txn_date, month, description, kind, amount, ref
          FROM bil_tenant_ledger
          WHERE tenant_id=:t AND date(txn_date) BETWEEN date(:s) AND date(:e)
          ORDER BY date(txn_date), id
        """), {"t": tenant_id, "s": start, "e": end}).mappings().all()
    else:
        rows = db.session.execute(text("""
          SELECT txn_date, month, description, kind, amount, ref
          FROM bil_tenant_ledger
          WHERE tenant_id=:t
          ORDER BY date(txn_date), id
        """), {"t": tenant_id}).mappings().all()

    # Running balance in Python (charges +, payments -)
    running = 0.0
    out = []
    for r in rows:
        amt = float(r["amount"] or 0.0)
        running += amt
        out.append({
            "date": r["txn_date"],
            "month": r["month"],
            "description": r["description"],
            "kind": r["kind"],
            "amount": amt,
            "ref": r["ref"],
            "balance": round(running, 2),
        })

    return render_template(
        "admin/billing/tenant_soa.html",
        tenant=tenant,
        rows=out,
        start=start,
        end=end,
        month=month,          # <-- add this
)

@admin_bp.route("/billing/tenant/payment/new", methods=["GET", "POST"])
def record_payment_form():
    tenant_id = request.values.get("tenant_id", type=int)
    if not tenant_id:
        abort(400)

    if request.method == "GET":
        return render_template("admin/billing/payment_form.html", tenant_id=tenant_id)

    # POST
    amount = request.form.get("amount", type=float)
    ref    = (request.form.get("reference") or "").strip()
    date_s = (request.form.get("date") or "").strip()   # YYYY-MM-DD

    if not amount or amount <= 0 or not date_s:
        flash("Please enter a valid date and positive amount.", "warning")
        return redirect(url_for("admin_bp.record_payment_form", tenant_id=tenant_id))

    # idempotency: prevent accidental double-capture of same payment
    db.session.execute(text("""
        INSERT OR IGNORE INTO bil_tenant_ledger
          (tenant_id, month, description, kind, debit, credit, txn_date, created_at)
        VALUES
          (:tid,
           strftime('%Y-%m', :dt),
           :desc,
           'payment',
           0,
           :amt,
           :dt,
           datetime('now'));
    """), {
        "tid": tenant_id,
        "dt":  date_s,
        "amt": amount,
        "desc": f"Payment {ref}" if ref else "Payment",
    })
    db.session.commit()

    flash("Payment recorded.", "success")
    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id))

@admin_bp.route("/billing/ledger/reverse_repost", methods=["POST"])
def ledger_reverse_repost():
    rid       = request.form.get("row_id", type=int)      # original ledger row id
    tenant_id = request.form.get("tenant_id", type=int)
    month     = request.form.get("month", type=str)

    # corrected values (can be same as original if just reversing)
    new_date  = request.form.get("txn_date") or db.session.execute(text("SELECT date('now')")).scalar()
    new_desc  = (request.form.get("description") or "").strip() or "Correction"
    new_kind  = (request.form.get("kind") or "adjustment").strip()
    new_amt   = request.form.get("amount", type=float)    # signed (+ charge, - payment)
    new_ref   = (request.form.get("ref") or "").strip()

    # fetch original
    row = db.session.execute(text("""
        SELECT tenant_id, txn_date, description, kind, amount, ref
        FROM bil_tenant_ledger WHERE id=:id
    """), {"id": rid}).mappings().first()
    if not row: abort(404)

    # reverse + repost
    with db.session.begin():
        db.session.execute(text("""
            INSERT INTO bil_tenant_ledger
              (tenant_id, txn_date, month, description, kind, amount, ref, created_at)
            VALUES
              (:tid, date('now'), :mon, 'Reversal of '||:desc, 'adjustment', :revamt, 'rev #'||:rid, datetime('now'))
        """), {"tid": row["tenant_id"], "mon": month, "desc": row["description"], "revamt": -float(row["amount"] or 0), "rid": rid})

        db.session.execute(text("""
            INSERT INTO bil_tenant_ledger
              (tenant_id, txn_date, month, description, kind, amount, ref, created_at)
            VALUES
              (:tid, :d, :mon, :desc, :kind, :amt, :ref, datetime('now'))
        """), {"tid": row["tenant_id"], "d": new_date, "mon": month, "desc": new_desc, "kind": new_kind, "amt": float(new_amt or 0), "ref": new_ref})

    flash("Reversed & reposted.", "success")
    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

@admin_bp.route("/billing/ledger/charge", methods=["POST"])
def tenant_ledger_charge():
    tenant_id = request.form.get("tenant_id", type=int)
    month     = request.form.get("month", type=str)
    amount    = abs(request.form.get("amount", type=float) or 0.0)
    desc      = (request.form.get("description") or "Sundry charge").strip()
    txn_date  = request.form.get("txn_date") or db.session.execute(text("SELECT date('now')")).scalar()
    ref       = (request.form.get("ref") or "").strip()

    if not tenant_id or amount <= 0:
        abort(400)

    db.session.execute(text("""
        INSERT INTO bil_tenant_ledger
          (tenant_id, txn_date, month, description, kind, amount, ref, created_at)
        VALUES
          (:tid, :d, :mon, :desc, 'charge', :amt, :ref, datetime('now'))
    """), {"tid": tenant_id, "d": txn_date, "mon": month, "desc": desc, "amt": amount, "ref": ref})
    db.session.commit()

    flash("Charge added.", "success")
    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

# --- helpers --------------------------------------------------------------

def _first_of(month_str: str) -> str:
    # 'YYYY-MM' -> 'YYYY-MM-01'
    return f"{month_str}-01"

def _ensure_month_charges(tenant_id: int, month: str):
    """
    Auto-posts rent (if scheduled) and any active recurring items for this month.
    Idempotent: uses NOT EXISTS/UNIQUE so you won’t get duplicates.
    """

    # 1) Rent (from bil_rent_schedule if present)
    db.session.execute(text("""
        INSERT INTO bil_tenant_ledger
          (tenant_id, month, txn_date, description, kind, amount, created_at)
        SELECT
          :tid, :mon, date(:mon || '-01'), 'Rent', 'charge', rs.amount, datetime('now')
        FROM bil_rent_schedule rs
        WHERE rs.tenant_id = :tid
          AND rs.month     = :mon
          AND rs.amount    > 0
          AND NOT EXISTS (
                SELECT 1 FROM bil_tenant_ledger
                WHERE tenant_id=:tid AND month=:mon
                  AND description='Rent' AND kind='charge'
          );
    """), {"tid": tenant_id, "mon": month})

    # Optional: mark the plan as posted
    db.session.execute(text("""
        UPDATE bil_rent_schedule
        SET is_posted = 1
        WHERE tenant_id=:tid AND month=:mon;
    """), {"tid": tenant_id, "mon": month})

    # 2) Recurring / Sundry active items windowed to this month
    db.session.execute(text("""
        INSERT INTO bil_tenant_ledger
          (tenant_id, month, txn_date, description, kind, amount, created_at)
        SELECT
          :tid,
          :mon,
          date(:mon || '-' || printf('%02d', COALESCE(NULLIF(ri.day_of_month,0),1))),
          ri.description,
          ri.kind,
          CASE WHEN ri.kind='credit' THEN -ABS(ri.amount) ELSE ABS(ri.amount) END,
          datetime('now')
        FROM bil_recurring_item ri
        WHERE ri.tenant_id = :tid
          AND ri.is_active = 1
          AND ri.start_month <= :mon
          AND (ri.end_month IS NULL OR ri.end_month >= :mon)
          AND NOT EXISTS (
              SELECT 1 FROM bil_tenant_ledger l
              WHERE l.tenant_id=:tid AND l.month=:mon
                AND l.description = ri.description
                AND l.kind        = ri.kind
          );
    """), {"tid": tenant_id, "mon": month})

    db.session.commit()


def _fetch_ledger_view(tenant_id: int):
    # Return rows already transformed for the template
    q = db.session.execute(text("""
        SELECT
          id,
          txn_date,
          description,
          kind,
          amount,
          ref
        FROM bil_tenant_ledger
        WHERE tenant_id = :tid
        ORDER BY txn_date, id
    """), {"tid": tenant_id}).mappings().all()

    items = []
    bal = 0.0
    for r in q:
        amt = float(r["amount"] or 0)
        bal += amt
        items.append({
            "id": r["id"],
            "txn_date": r["txn_date"],
            "description": r["description"],
            "kind": r["kind"],
            "charge": amt if amt > 0 else None,
            "payment": (-amt) if amt < 0 else None,
            "ref": r["ref"],
            "balance": round(bal, 2),
        })
    return items, round(bal, 2)


# --- ledger page (unchanged URL), now auto-applies rent/recurring ----------

@admin_bp.route("/billing/ledger", methods=["GET"])
def tenant_ledger():
    tenant_id = request.args.get("tenant_id", type=int)
    month     = request.args.get("month", type=str)
    if not tenant_id or not month:
        abort(400)

    # optional: ensure recurring rent auto-posts for this month
    try:
        from app.utils.billing_helpers import ensure_recurring_materialized
        ensure_recurring_materialized(tenant_id, month)
    except Exception:
        pass  # don't crash the page

    tenant = db.session.execute(
        text("SELECT id, name, unit_label FROM bil_tenant WHERE id=:tid"),
        {"tid": tenant_id}
    ).mappings().first() or abort(404)

    rows = db.session.execute(
        text("""
          SELECT id, tenant_id, txn_date, description, kind, ref, amount
          FROM bil_tenant_ledger
          WHERE tenant_id = :tid
          ORDER BY date(txn_date), id
        """),
        {"tid": tenant_id},
    ).mappings().all()

    balance = 0.0
    items = []
    for r in rows:
        amt = float(r["amount"] or 0.0)
        balance += amt
        items.append({
            "id": r["id"],
            "txn_date": r["txn_date"],
            "description": r["description"],
            "kind": r["kind"],
            "ref": r["ref"],
            "auto": (r["ref"] or "").startswith("AUTO:REC:"),
            "charge": amt if amt > 0 else None,
            "payment": (-amt) if amt < 0 else None,
            "balance": balance,
        })


    # 🔑 This powers the right-hand Recurring panel
    recurring = db.session.execute(
        text("""
          SELECT id, description, kind, amount, day_of_month, is_active
          FROM bil_tenant_recurring
          WHERE tenant_id = :tid
          ORDER BY description
        """),
        {"tid": tenant_id},
    ).mappings().all()

    today_str = date.today().isoformat()

    # 🔑 Render the template that *has* Payments + Recurring panels
    return render_template(
        "admin/billing/tenant_ledger.html",
        tenant=tenant, month=month,
        items=items, balance=balance,
        recurring=recurring,
        today=today_str,
    )
# --- payments: simple insert (NO upsert; avoids the ON CONFLICT error) -----

@admin_bp.route("/billing/ledger/payment", methods=["POST"])
def tenant_ledger_payment():
    tenant_id = request.form.get("tenant_id", type=int)
    month     = request.form.get("month", type=str)
    amt       = request.form.get("amount", type=float)
    txn_date  = request.form.get("txn_date", type=str) or _first_of(month)
    ref       = (request.form.get("ref") or "").strip()

    if not tenant_id or not month or not amt or amt <= 0:
        flash("Amount required.", "warning")
        return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

    try:
        db.session.execute(text("""
            INSERT INTO bil_tenant_ledger
              (tenant_id, month, txn_date, description, kind, amount, ref, created_at)
            VALUES
              (:tid, :mon, :dt, 'Payment', 'payment', :neg_amt, :ref, datetime('now'))
        """), {"tid": tenant_id, "mon": month, "dt": txn_date, "neg_amt": -abs(amt), "ref": ref})
        db.session.commit()
        flash(f"Payment recorded: {amt:.2f}", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Database error while posting to ledger: {e.__class__.__name__}", "danger")

    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))


# --- recurring: add and toggle --------------------------------------------

@admin_bp.route("/billing/recurring/add", methods=["POST"])
def tenant_recurring_add():
    tenant_id = request.form.get("tenant_id", type=int)
    month     = request.form.get("month", type=str)  # for redirect
    desc      = (request.form.get("description") or "").strip()
    kind      = request.form.get("kind") or "charge"
    amount    = request.form.get("amount", type=float)
    day       = request.form.get("day_of_month", type=int) or 1
    start_m   = request.form.get("start_month") or month
    end_m     = request.form.get("end_month") or None

    if not tenant_id or not month or not desc or not amount:
        flash("Description and amount required.", "warning")
        return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

    try:
        db.session.execute(text("""
            INSERT INTO bil_recurring_item
              (tenant_id, description, kind, amount, day_of_month, start_month, end_month, is_active)
            VALUES
              (:tid, :desc, :kind, :amt, :day, :sm, :em, 1)
        """), {"tid": tenant_id, "desc": desc, "kind": kind, "amt": amount, "day": day, "sm": start_m, "em": end_m})
        db.session.commit()
        flash("Recurring item added.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error adding recurring item: {e.__class__.__name__}", "danger")

    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))


@admin_bp.route("/billing/recurring/toggle", methods=["POST"])
def tenant_recurring_toggle():
    tenant_id = request.form.get("tenant_id", type=int)
    month     = request.form.get("month", type=str)
    item_id   = request.form.get("item_id", type=int)
    if not tenant_id or not month or not item_id:
        abort(400)

    try:
        db.session.execute(text("""
            UPDATE bil_recurring_item
            SET is_active = CASE WHEN is_active=1 THEN 0 ELSE 1 END
            WHERE id = :id
        """), {"id": item_id})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        flash(f"Error toggling item: {e.__class__.__name__}", "danger")

    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))
'''
@admin_bp.route("/billing/ledger/item/<int:item_id>/edit", methods=["GET", "POST"])
def tenant_ledger_item_edit(item_id: int):
    row = db.session.execute(
        text("SELECT * FROM bil_tenant_ledger WHERE id=:id"),
        {"id": item_id},
    ).mappings().first()
    if not row:
        abort(404)

    tenant_id = row["tenant_id"]
    month_default = (row.get("month") or (row.get("txn_date") or "")[:7] or "")
    month = request.values.get("month", month_default)

    if request.method == "POST":
        description = (request.form.get("description") or "").strip()
        kind        = (request.form.get("kind") or "charge").strip()
        amount_in   = float(request.form.get("amount") or 0)
        txn_date    = request.form.get("txn_date") or row["txn_date"]
        ref         = request.form.get("ref") or None

        # Signed amount convention: charges/adjustments positive, payments/credits negative
        if kind in ("payment", "credit"):
            signed = -abs(amount_in)
        else:
            signed = abs(amount_in)

        db.session.execute(
            text("""
              UPDATE bil_tenant_ledger
              SET description = :d,
                  kind        = :k,
                  amount      = :a,
                  txn_date    = :dt,
                  month       = substr(:dt,1,7),
                  ref         = :r
              WHERE id = :id
            """),
            {"d": description, "k": kind, "a": signed, "dt": txn_date, "r": ref, "id": item_id},
        )
        db.session.commit()
        flash("Ledger entry updated.", "success")
        return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

    # GET – show form with absolute amount as a convenience
    abs_amount = float(row["amount"] or 0.0)
    if abs_amount < 0:
        abs_amount = -abs_amount

    tenant = db.session.get(BilTenant, tenant_id)
    return render_template(
        "admin/billing/tenant_ledger_edit.html",
        row=row, amount=abs_amount, tenant=tenant, month=month,
    )

@admin_bp.route("/admin/billing/ledger/item/<int:item_id>/delete", methods=["POST"])
def tenant_ledger_item_delete(item_id: int):
    row = db.session.execute(
        text("SELECT id, tenant_id, month, txn_date FROM bil_tenant_ledger WHERE id=:id"),
        {"id": item_id},
    ).mappings().first()
    if not row:
        abort(404)

    tenant_id = row["tenant_id"]
    month     = request.form.get("month") or (row.get("month") or (row.get("txn_date") or "")[:7])

    db.session.execute(text("DELETE FROM bil_tenant_ledger WHERE id=:id"), {"id": item_id})
    db.session.commit()
    flash("Ledger entry deleted.", "success")
    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))
'''

@admin_bp.route("/billing/ledger/item/<int:item_id>/edit", methods=["GET","POST"])
def tenant_ledger_item_edit(item_id):
    tenant_id = request.args.get("tenant_id", type=int)
    month = request.args.get("month")

    row = db.session.execute(
        text("SELECT * FROM bil_tenant_ledger WHERE id=:id"),
        {"id": item_id}
    ).mappings().first()
    if not row:
        abort(404)

    # Lock recurring rows
    if (row["ref"] or "").startswith("AUTO:REC:"):
        flash("This is an auto-posted recurring row. Edit the recurring definition instead.", "warning")
        return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

    if request.method == "POST":
        db.session.execute(
            text("""
                UPDATE bil_tenant_ledger
                SET txn_date=:d, description=:desc, kind=:k, ref=:r, amount=:a
                WHERE id=:id
            """),
            {
                "id": item_id,
                "d": request.form.get("txn_date"),
                "desc": request.form.get("description"),
                "k": request.form.get("kind"),
                "r": request.form.get("ref"),
                "a": request.form.get("amount"),
            },
        )
        db.session.commit()
        flash("Ledger item updated.", "success")
        return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

    return render_template("admin/billing/tenant_item_edit.html", row=row, tenant_id=tenant_id, month=month)


def _month_bounds(ym: str):
    y, m = [int(x) for x in ym.split("-")]
    start = date(y, m, 1)
    last = calendar.monthrange(y, m)[1]
    end = date(y, m, last)
    return start, end

def _clamp_dom(y: int, m: int, d: int) -> date:
    last = calendar.monthrange(y, m)[1]
    return date(y, m, min(max(1, d), last))

def materialize_recurring_for_month_sql(tenant_id: int, month_ym: str):
    """
    Insert one ledger row per active recurring item for the given month (YYYY-MM),
    using a deterministic ref 'AUTO:REC:<rec_id>:YYYY-MM' to avoid duplicates.
    Pure SQL; no ORM models required.
    """
    start_d, end_d = _month_bounds(month_ym)
    y, m = start_d.year, start_d.month

    recurs = db.session.execute(
        text("""
            SELECT id, description, kind, amount, day_of_month, is_active
            FROM bil_tenant_recurring
            WHERE tenant_id = :tid AND is_active = 1
        """),
        {"tid": tenant_id},
    ).mappings().all()

    for r in recurs:
        rec_id = r["id"]
        auto_ref = f"AUTO:REC:{rec_id}:{month_ym}"

        # already posted for this month?
        exists = db.session.execute(
            text("""
                SELECT id FROM bil_tenant_ledger
                WHERE tenant_id = :tid
                  AND ref = :ref
                  AND date(txn_date) BETWEEN date(:start_d) AND date(:end_d)
                LIMIT 1
            """),
            {"tid": tenant_id, "ref": auto_ref,
             "start_d": start_d.isoformat(), "end_d": end_d.isoformat()},
        ).first()
        if exists:
            continue

        post_dt = _clamp_dom(y, m, int(r["day_of_month"] or 1)).isoformat()
        kind = r["kind"]                     # 'charge' or 'credit' (or 'payment')
        amt  = float(r["amount"] or 0.0)
        signed_amount = amt if kind == "charge" else -amt

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
                "desc": r["description"],
                "kind": kind,
                "ref": auto_ref,          # used to mark row as auto/locked in UI
                "amount": signed_amount,
            },
        )

    db.session.commit()

@admin_bp.route("/billing/ledger/item/add", methods=["POST"])
def tenant_ledger_item_add():
    """
    Add a single ledger row (typically a Payment from the right-hand panel).
    Expects form fields: tenant_id, month (YYYY-MM), txn_date (YYYY-MM-DD),
                         description, kind ('charge' or 'payment'), amount, ref (optional).
    Payments are stored as negative amounts.
    """
    tenant_id = request.form.get("tenant_id", type=int)
    month     = request.form.get("month", type=str)
    if not tenant_id or not month:
        abort(400)
        # keep ledger in sync with Recurring for this month
    apply_recurring_to_ledger(tenant_id, month)

    txn_date    = request.form.get("txn_date") or date.today().isoformat()
    description = (request.form.get("description") or "").strip() or "Entry"
    kind        = (request.form.get("kind") or "payment").strip().lower()   # default to payment
    ref         = (request.form.get("ref") or "").strip()

    # Parse amount and sign it
    try:
        raw_amount = float((request.form.get("amount") or "0").replace(",", ""))
    except ValueError:
        flash("Invalid amount.", "warning")
        return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

    amount = raw_amount if kind == "charge" else -abs(raw_amount)

    # Insert
    db.session.execute(
        text("""
            INSERT INTO bil_tenant_ledger
                (tenant_id, month, description, kind, amount, debit, credit, txn_date, ref, created_at)
            VALUES
                (:tid, :mon, :desc, :kind, :amt,
                 CASE WHEN :amt > 0 THEN :amt ELSE 0 END,
                 CASE WHEN :amt < 0 THEN ABS(:amt) ELSE 0 END,
                 :txn_date, :ref, datetime('now'))
        """),
        {
            "tid": tenant_id,
            "mon": month,
            "desc": description,
            "kind": kind,
            "amt": amount,
            "txn_date": txn_date,
            "ref": ref or None,
        },
    )
    db.session.commit()
    flash("Ledger item added.", "success")

    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

# routes.py



@admin_bp.route("/billing/ledger/payment/new", methods=["GET", "POST"])
def payment_new():
    tenant_id = request.values.get("tenant_id", type=int)
    month     = request.values.get("month", type=str)
    if not tenant_id or not month:
        abort(400)

    if request.method == "POST":
        txn_date    = (request.form.get("txn_date") or date.today().isoformat()).strip()
        kind        = (request.form.get("kind") or "payment").strip().lower()
        description = (request.form.get("description") or "Tenant Payment").strip()[:255]
        ref         = (request.form.get("ref") or "").strip()[:100]

        amt_text = (request.form.get("amount") or "").replace(",", "").strip()
        try:
            raw_amount = float(amt_text)
        except ValueError:
            flash("Please enter a valid amount.", "warning")
            return redirect(url_for("admin_bp.payment_new", tenant_id=tenant_id, month=month))

        # charges positive; payments negative
        amount = raw_amount if kind == "charge" else -abs(raw_amount)

        def do_insert(desc_text: str):
            db.session.execute(
                text("""
                    INSERT INTO bil_tenant_ledger
                        (tenant_id, month, description, kind, amount,
                         debit, credit, txn_date, ref, created_at)
                    VALUES
                        (:tid, :mon, :desc, :kind, :amt,
                         CASE WHEN :amt > 0 THEN :amt ELSE 0 END,
                         CASE WHEN :amt < 0 THEN ABS(:amt) ELSE 0 END,
                         :d, :ref, datetime('now'))
                """),
                {
                    "tid": tenant_id, "mon": month, "desc": desc_text, "kind": kind,
                    "amt": amount, "d": txn_date, "ref": (ref or None),
                },
            )

        try:
            # First try as-is
            do_insert(description)
            db.session.commit()
            flash("Entry saved.", "success")

        except IntegrityError as e:
            db.session.rollback()
            # Likely the broad unique index still exists; auto-unique the description and retry once
            if "UNIQUE constraint failed: bil_tenant_ledger.tenant_id" in str(e):
                unique_desc = f"{description} [{txn_date}"
                if ref:
                    unique_desc += f" • {ref}"
                unique_desc += "]"
                unique_desc = unique_desc[:255]
                try:
                    do_insert(unique_desc)
                    db.session.commit()
                    flash("Entry saved.", "success")
                except IntegrityError as e2:
                    db.session.rollback()
                    current_app.logger.exception("payment_new unique retry failed")
                    flash("Could not save entry (unique constraint). Please change description or fix index.", "danger")
                    return redirect(url_for("admin_bp.payment_new", tenant_id=tenant_id, month=month))
            else:
                current_app.logger.exception("payment_new insert failed")
                flash(f"Could not save entry: {getattr(e, 'orig', e)}", "danger")
                return redirect(url_for("admin_bp.payment_new", tenant_id=tenant_id, month=month))

        except SQLAlchemyError as e:
            db.session.rollback()
            current_app.logger.exception("payment_new insert failed (sqlalchemy)")
            flash(f"Could not save entry: {getattr(e, 'orig', e)}", "danger")
            return redirect(url_for("admin_bp.payment_new", tenant_id=tenant_id, month=month))

        # back to ledger (re-queries display)
        return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

    # GET → show form
    return render_template(
        "admin/billing/payment_new.html",
        tenant_id=tenant_id,
        month=month,
        today=date.today().isoformat(),
    )

# --- ONE-TIME: fix the ledger unique index so only METSOA is unique ---

@admin_bp.route("/billing/_fix_uq_metro", methods=["GET"])
def _fix_uq_metro():
    """
    Drops the broad unique index on (tenant_id, month, kind, description)
    and recreates a *filtered* unique index that applies ONLY to the
    METSOA row: description='Due to Metro' AND kind='charge'.
    Then shows current indexes so you can confirm.
    """
    out = []
    try:
        # 1) Drop the old broad index (safe if it doesn't exist)
        db.session.execute(text("DROP INDEX IF EXISTS uq_ledger_metro;"))
        out.append("Dropped uq_ledger_metro (if existed).")

        # 2) Create filtered unique index (SQLite 3.8+ supports WHERE on indexes)
        db.session.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_ledger_metro
            ON bil_tenant_ledger (tenant_id, month, kind, description)
            WHERE description = 'Due to Metro' AND kind = 'charge';
        """))
        out.append("Created filtered uq_ledger_metro (Due to Metro + charge).")

        db.session.commit()

        # 3) Show current index list + index SQL for verification
        idx_list = db.session.execute(text("PRAGMA index_list('bil_tenant_ledger');")).fetchall()
        out.append(f"PRAGMA index_list: {idx_list}")

        idx_sql = db.session.execute(
            text("SELECT sql FROM sqlite_master WHERE type='index' AND name='uq_ledger_metro';")
        ).fetchone()
        out.append(f"Index SQL: {idx_sql[0] if idx_sql and idx_sql[0] else 'N/A'}")

    except Exception as e:
        db.session.rollback()
        out.append(f"ERROR: {e}")

    return "<pre>" + "\n".join(out) + "</pre>"

@admin_bp.route("/billing/ledger/item/<int:item_id>/delete", methods=["POST"])
def tenant_ledger_item_delete(item_id: int):
    """
    Delete a single ledger row safely.
    Requires tenant_id & month so we can redirect back to the same page.
    Refuses to delete locked rows (e.g., recurring auto-posts).
    """
    tenant_id = request.values.get("tenant_id", type=int)
    month     = request.values.get("month", type=str)
    if not tenant_id or not month:
        abort(400)

    # refuse to delete locked rows
    row = db.session.execute(
        text("""
            SELECT id, tenant_id, ref
            FROM bil_tenant_ledger
            WHERE id = :id AND tenant_id = :tid
        """),
        {"id": item_id, "tid": tenant_id},
    ).mappings().first()

    if not row:
        flash("Item not found.", "warning")
        return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

    if row["ref"] and str(row["ref"]).startswith("AUTO:REC:"):
        flash("This item is locked and cannot be deleted.", "warning")
        return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

    # delete
    db.session.execute(
        text("DELETE FROM bil_tenant_ledger WHERE id = :id AND tenant_id = :tid"),
        {"id": item_id, "tid": tenant_id},
    )
    db.session.commit()
    flash("Ledger item deleted.", "success")

    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))

# ---------- LIST ----------
@admin_bp.route("/billing/recurring", methods=["GET"])
def recurring_index():
    tenant_id = request.args.get("tenant_id", type=int)
    month     = request.args.get("month", type=str)  # optional, used for the Back link
    if not tenant_id:
        abort(400)

    rows = db.session.execute(
        text("""
          SELECT id, description, kind, amount, day_of_month, is_active,
                 COALESCE(start_month,'') AS start_month,
                 COALESCE(end_month,'')   AS end_month
          FROM bil_tenant_recurring
          WHERE tenant_id = :tid
          ORDER BY description, id
        """),
        {"tid": tenant_id},
    ).mappings().all()

    return render_template(
        "admin/billing/recurring_index.html",
        tenant_id=tenant_id, month=month, rows=rows
    )

# ---------- CREATE ----------
@admin_bp.route("/billing/recurring/new", methods=["GET", "POST"])
def recurring_new():
    tenant_id = request.values.get("tenant_id", type=int)
    month     = request.values.get("month", type=str)  # optional
    if not tenant_id:
        abort(400)

    if request.method == "POST":
        description = (request.form.get("description") or "Recurring").strip()[:255]
        kind        = (request.form.get("kind") or "charge").strip().lower()
        day_of_month = request.form.get("day_of_month", type=int) or 1
        start_month  = (request.form.get("start_month") or "")[:7] or None
        end_month    = (request.form.get("end_month") or "")[:7] or None

        amt_text = (request.form.get("amount") or "").replace(",", "").strip()
        try:
            amount = float(amt_text)
        except ValueError:
            flash("Amount must be a number.", "warning")
            return redirect(url_for("admin_bp.recurring_new", tenant_id=tenant_id, month=month))

        # normalize kind: if it's a credit/payment recurring, store negative
        if kind == "payment" or kind == "credit":
            amount = -abs(amount)

        db.session.execute(
            text("""
              INSERT INTO bil_tenant_recurring
                (tenant_id, description, kind, amount, day_of_month,
                 start_month, end_month, is_active)
              VALUES
                (:tid, :desc, :kind, :amt, :day, :sm, :em, 1)
            """),
            {
                "tid": tenant_id, "desc": description, "kind": kind,
                "amt": amount, "day": day_of_month,
                "sm": start_month, "em": end_month,
            },
        )
        db.session.commit()
        flash("Recurring item created.", "success")
        return redirect(url_for("admin_bp.recurring_index", tenant_id=tenant_id, month=month))

    # GET
    return render_template(
        "admin/billing/recurring_form.html",
        tenant_id=tenant_id, month=month, mode="new",
        today=date.today().isoformat()
    )

# ---------- EDIT ----------
@admin_bp.route("/billing/recurring/<int:rec_id>/edit", methods=["GET", "POST"])
def recurring_edit(rec_id: int):
    tenant_id = request.values.get("tenant_id", type=int)
    month     = request.values.get("month", type=str)  # optional
    if not tenant_id:
        abort(400)

    row = db.session.execute(
        text("""
          SELECT id, tenant_id, description, kind, amount, day_of_month,
                 COALESCE(start_month,'') AS start_month,
                 COALESCE(end_month,'')   AS end_month,
                 is_active
          FROM bil_tenant_recurring
          WHERE id = :id AND tenant_id = :tid
        """),
        {"id": rec_id, "tid": tenant_id},
    ).mappings().first()
    if not row:
        flash("Recurring item not found.", "warning")
        return redirect(url_for("admin_bp.recurring_index", tenant_id=tenant_id, month=month))

    if request.method == "POST":
        description = (request.form.get("description") or "Recurring").strip()[:255]
        kind        = (request.form.get("kind") or "charge").strip().lower()
        day_of_month = request.form.get("day_of_month", type=int) or 1
        start_month  = (request.form.get("start_month") or "")[:7] or None
        end_month    = (request.form.get("end_month") or "")[:7] or None

        amt_text = (request.form.get("amount") or "").replace(",", "").strip()
        try:
            amount = float(amt_text)
        except ValueError:
            flash("Amount must be a number.", "warning")
            return redirect(url_for("admin_bp.recurring_edit", rec_id=rec_id, tenant_id=tenant_id, month=month))

        if kind == "payment" or kind == "credit":
            amount = -abs(amount)

        db.session.execute(
            text("""
              UPDATE bil_tenant_recurring
              SET description = :desc,
                  kind        = :kind,
                  amount      = :amt,
                  day_of_month= :day,
                  start_month = :sm,
                  end_month   = :em
              WHERE id = :id AND tenant_id = :tid
            """),
            {
                "id": rec_id, "tid": tenant_id,
                "desc": description, "kind": kind, "amt": amount,
                "day": day_of_month, "sm": start_month, "em": end_month,
            },
        )
        db.session.commit()
        flash("Recurring item updated.", "success")
        return redirect(url_for("admin_bp.recurring_index", tenant_id=tenant_id, month=month))

    # GET
    return render_template(
        "admin/billing/recurring_form.html",
        tenant_id=tenant_id, month=month, mode="edit", rec=row
    )

# ---------- TOGGLE ACTIVE ----------
@admin_bp.route("/billing/recurring/<int:rec_id>/toggle", methods=["POST"])
def recurring_toggle(rec_id: int):
    tenant_id = request.values.get("tenant_id", type=int)
    month     = request.values.get("month", type=str)
    if not tenant_id:
        abort(400)

    db.session.execute(
        text("""
          UPDATE bil_tenant_recurring
          SET is_active = CASE WHEN is_active=1 THEN 0 ELSE 1 END
          WHERE id=:id AND tenant_id=:tid
        """),
        {"id": rec_id, "tid": tenant_id},
    )
    db.session.commit()
    return redirect(url_for("admin_bp.recurring_index", tenant_id=tenant_id, month=month))

# ---------- DELETE ----------
@admin_bp.route("/billing/recurring/<int:rec_id>/delete", methods=["POST"])
def recurring_delete(rec_id: int):
    tenant_id = request.values.get("tenant_id", type=int)
    month     = request.values.get("month", type=str)
    if not tenant_id:
        abort(400)

    db.session.execute(
        text("DELETE FROM bil_tenant_recurring WHERE id=:id AND tenant_id=:tid"),
        {"id": rec_id, "tid": tenant_id},
    )
    db.session.commit()
    flash("Recurring item deleted.", "success")
    return redirect(url_for("admin_bp.recurring_index", tenant_id=tenant_id, month=month))

def _first_valid_post_date(period_month: str, day_of_month: int | None) -> str:
    y, m = map(int, period_month.split("-"))
    dom = max(1, min(day_of_month or 1, calendar.monthrange(y, m)[1]))
    return date(y, m, dom).isoformat()

def apply_recurring_to_ledger(tenant_id: int, period_month: str) -> None:
    """
    Make ledger for (tenant, month) match Recurring table exactly.
    - Remove any previous auto recurring rows for that month.
    - Insert rows for ALL active recurring entries that are in-range.
    We tag auto rows with ref like 'AUTO:REC:<rec_id>'.
    """
    # wipe old auto rows for that month
    db.session.execute(
        text("""
            DELETE FROM bil_tenant_ledger
            WHERE tenant_id = :tid AND month = :mon AND ref LIKE 'AUTO:REC:%'
        """),
        {"tid": tenant_id, "mon": period_month},
    )

    # load recurring
    recs = db.session.execute(
        text("""
          SELECT id, description, kind, amount, day_of_month, is_active,
                 COALESCE(start_month,'') AS start_month,
                 COALESCE(end_month,'')   AS end_month
          FROM bil_tenant_recurring
          WHERE tenant_id = :tid
        """),
        {"tid": tenant_id},
    ).mappings().all()

    for r in recs:
        if not r["is_active"]:
            continue
        # month inclusion
        sm = r["start_month"] or None
        em = r["end_month"] or None
        if sm and period_month < sm:
            continue
        if em and period_month > em:
            continue

        amt = float(r["amount"] or 0.0)
        if (r["kind"] or "").lower() in ("payment", "credit"):
            amt = -abs(amt)  # credits negative
            kind = "payment"
        else:
            amt = abs(amt)   # charges positive
            kind = "charge"

        post_date = _first_valid_post_date(period_month, r["day_of_month"])
        ref = f"AUTO:REC:{r['id']}"

        db.session.execute(
            text("""
                INSERT INTO bil_tenant_ledger
                (tenant_id, month, description, kind, amount,
                 debit, credit, txn_date, ref, created_at)
                VALUES
                (:tid, :mon, :desc, :kind, :amt,
                 CASE WHEN :amt > 0 THEN :amt ELSE 0 END,
                 CASE WHEN :amt < 0 THEN ABS(:amt) ELSE 0 END,
                 :d, :ref, datetime('now'))
            """),
            {
                "tid": tenant_id,
                "mon": period_month,
                "desc": r["description"],
                "kind": kind,
                "amt": amt,
                "d": post_date,
                "ref": ref,
            },
        )

    db.session.commit()

@admin_bp.route("/billing/recurring/apply", methods=["POST"])
def recurring_apply():
    tenant_id = request.values.get("tenant_id", type=int)
    month     = request.values.get("month", type=str)
    if not tenant_id or not month:
        abort(400)
    apply_recurring_to_ledger(tenant_id, month)
    flash("Recurring reapplied to ledger.", "success")
    return redirect(url_for("admin_bp.tenant_ledger", tenant_id=tenant_id, month=month))


# ---------- helpers ----------

def _get_or_create_session(label: str) -> int:
    row = db.session.execute(
        text("SELECT id FROM muni_recon_session WHERE label=:lbl"),
        {"lbl": label},
    ).fetchone()
    if row:
        return row[0]
    db.session.execute(
        text("INSERT INTO muni_recon_session(label) VALUES(:lbl)"),
        {"lbl": label},
    )
    db.session.commit()
    return db.session.execute(
        text("SELECT id FROM muni_recon_session WHERE label=:lbl"),
        {"lbl": label},
    ).fetchone()[0]

def _unmatched_query(side: str, session_id: int, date_from: str, date_to: str):
    # side = 'left' or 'right', join against matches + exclusions
    if side == "left":
        base = "v_recon_left"
        side_col = "left"
        src_name = "left_src"
        id_name  = "left_id"
    else:
        base = "v_recon_right"
        side_col = "right"
        src_name = "right_src"
        id_name  = "right_id"

    sql = f"""
    SELECT s.id, s.txn_date, s.description, s.amount
    FROM {base} s
    WHERE date(s.txn_date) BETWEEN date(:d1) AND date(:d2)
      AND NOT EXISTS (
        SELECT 1 FROM muni_recon_match m
        WHERE m.session_id=:sid AND m.{src_name}=:src AND m.{id_name}=s.id
      )
      AND NOT EXISTS (
        SELECT 1 FROM muni_recon_exclusion e
        WHERE e.session_id=:sid AND e.side=:side AND e.src=:src AND e.src_id=s.id
      )
    ORDER BY date(s.txn_date), s.id
    """
    return db.session.execute(text(sql), {
        "sid": session_id, "d1": date_from, "d2": date_to,
        "src": base, "side": side_col
    }).mappings().all()

def _abs(x): return abs(float(x or 0.0))

# ---------- UI: dashboard ----------
@admin_bp.route("/muni/recon", methods=["GET"])
def muni_recon():
    # month label e.g. '2025-06'
    label = request.args.get("month", type=str)
    if not label:
        abort(400)  # require month

    session_id = _get_or_create_session(label)

    # month bounds
    y, m = [int(x) for x in label.split("-")]
    d1 = datetime(y, m, 1)
    if m == 12:
        d2 = datetime(y+1, 1, 1) - timedelta(days=1)
    else:
        d2 = datetime(y, m+1, 1) - timedelta(days=1)
    date_from, date_to = d1.date().isoformat(), d2.date().isoformat()

    left  = _unmatched_query("left",  session_id, date_from, date_to)
    right = _unmatched_query("right", session_id, date_from, date_to)

    # gather matches for this month
    matches = db.session.execute(
        text("""
        SELECT m.id, m.amount, m.rule,
               l.txn_date AS l_date, l.description AS l_desc,
               r.txn_date AS r_date, r.description AS r_desc
        FROM muni_recon_match m
        JOIN v_recon_left  l ON (m.left_src='v_recon_left'  AND m.left_id=l.id)
        JOIN v_recon_right r ON (m.right_src='v_recon_right' AND m.right_id=r.id)
        WHERE m.session_id=:sid
        ORDER BY m.id DESC
        """), {"sid": session_id}
    ).mappings().all()

    return render_template(
        "admin/muni/recon.html",
        label=label, session_id=session_id,
        left=left, right=right, matches=matches
    )

# ---------- Auto-match ----------
@admin_bp.route("/muni/recon/auto", methods=["POST"])
def muni_recon_auto():
    label = request.form.get("month")
    if not label:
        abort(400)
    session_id = _get_or_create_session(label)

    # month bounds
    y, m = [int(x) for x in label.split("-")]
    d1 = datetime(y, m, 1)
    d2 = (datetime(y+1,1,1) - timedelta(days=1)) if m==12 else (datetime(y,m+1,1)-timedelta(days=1))
    date_from, date_to = d1.date().isoformat(), d2.date().isoformat()

    left  = _unmatched_query("left",  session_id, date_from, date_to)
    right = _unmatched_query("right", session_id, date_from, date_to)

    # index right by amount within ±3 days
    by_amount = {}
    for r in right:
        key = round(_abs(r["amount"]), 2)
        by_amount.setdefault(key, []).append(r)

    matched = 0
    for L in left:
        key = round(_abs(L["amount"]), 2)
        if key not in by_amount:
            continue
        # pick a right within ±3 days if possible
        ldt = datetime.fromisoformat(L["txn_date"])
        pick = None
        for idx, R in enumerate(by_amount[key]):
            rdt = datetime.fromisoformat(R["txn_date"])
            if abs((rdt - ldt).days) <= 3:
                pick = by_amount[key].pop(idx)
                break
        if not pick and by_amount[key]:
            pick = by_amount[key].pop(0)  # fallback same amount
        if not pick:
            continue

        db.session.execute(text("""
            INSERT OR IGNORE INTO muni_recon_match
              (session_id, left_src, left_id, right_src, right_id, amount, rule)
            VALUES
              (:sid, 'v_recon_left', :lid, 'v_recon_right', :rid, :amt, 'auto:amount+date±3d')
        """), {"sid": session_id, "lid": L["id"], "rid": pick["id"], "amt": round(_abs(L["amount"]),2)})
        matched += 1

    db.session.commit()
    flash(f"Auto-matched {matched} pairs.", "success")
    return redirect(url_for("admin_bp.muni_recon", month=label))

# ---------- Manual match ----------
@admin_bp.route("/muni/recon/match", methods=["POST"])
def muni_recon_match():
    label = request.form.get("month")
    session_id = _get_or_create_session(label)
    left_id  = request.form.get("left_id", type=int)
    right_id = request.form.get("right_id", type=int)
    if not (left_id and right_id and label):
        abort(400)

    # fetch amounts to store absolute
    L = db.session.execute(text("SELECT amount FROM v_recon_left  WHERE id=:id"),  {"id": left_id}).fetchone()
    R = db.session.execute(text("SELECT amount FROM v_recon_right WHERE id=:id"),  {"id": right_id}).fetchone()
    if not L or not R:
        flash("Items not found.", "warning")
        return redirect(url_for("admin_bp.muni_recon", month=label))

    amt = round(_abs(L[0]), 2)
    if amt != round(_abs(R[0]), 2):
        flash("Amounts differ; matched anyway (manual).", "warning")

    db.session.execute(text("""
        INSERT OR IGNORE INTO muni_recon_match
          (session_id, left_src, left_id, right_src, right_id, amount, rule)
        VALUES (:sid, 'v_recon_left', :lid, 'v_recon_right', :rid, :amt, 'manual')
    """), {"sid": session_id, "lid": left_id, "rid": right_id, "amt": amt})
    db.session.commit()
    return redirect(url_for("admin_bp.muni_recon", month=label))

# ---------- Unmatch ----------
@admin_bp.route("/muni/recon/unmatch/<int:mid>", methods=["POST"])
def muni_recon_unmatch(mid: int):
    label = request.form.get("month")
    db.session.execute(text("DELETE FROM muni_recon_match WHERE id=:id"), {"id": mid})
    db.session.commit()
    return redirect(url_for("admin_bp.muni_recon", month=label))

@admin_bp.route("/billing/export", methods=["GET"])
def billing_export_picker():
    """
    Simple month picker that lets you export:
    - Muni Recon CSV (calls /admin/muni/recon/export?month=YYYY-MM)
    You can extend it later with other exports.
    """
    from datetime import date
    default_month = date.today().strftime("%Y-%m")  # prefill
    return render_template("admin/billing/export_picker.html", default_month=default_month)

def _ensure_recon_views():
    missing = db.session.execute(text("""
        WITH v(name) AS (VALUES ('v_recon_left'), ('v_recon_right'))
        SELECT name FROM v
        WHERE NOT EXISTS (
          SELECT 1 FROM sqlite_master WHERE type IN ('view','table') AND name=v.name
        );
    """)).fetchall()
    if missing:
        names = ", ".join([m[0] for m in missing])
        flash(f"Reconciliation views missing: {names}. Please create/replace them.", "danger")
        abort(500)

# app/school_billing/admin_routes.py (or wherever your admin_bp lives)
@admin_bp.route("/billing/muni/<account_number>/ledger")
def muni_ledger(account_number):
    rows = db.session.execute(text("""
        SELECT period, balance, due, paid, arrears
        FROM v_admin_muni_ledger
        WHERE account_number = :acc
        ORDER BY period
    """), {"acc": account_number}).mappings().all()

    recon = db.session.execute(text("""
        SELECT period, system_due, metro_due, diff
        FROM v_muni_due_vs_metsoa
        WHERE account_number = :acc
        ORDER BY period
    """), {"acc": account_number}).mappings().all()

    return render_template(
        "admin/billing/muni/ledger.html",
        account_number=account_number,
        rows=rows,
        recon=recon
    )

PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")  # YYYY-MM

def validate_period(p: str) -> str:
    if not p or not PERIOD_RE.match(p):
        raise ValueError("Period must be in YYYY-MM format.")
    return p

def get_account_id(account_number: str) -> int | None:
    row = db.session.execute(
        text("SELECT id FROM bil_muni_account WHERE account_number = :acc"),
        {"acc": account_number},
    ).fetchone()
    return row[0] if row else None

# app/school_billing/admin_routes.py  (or wherever admin_bp lives)

@admin_bp.route("/billing/muni/go", methods=["POST"])
def muni_accounts_go():
    """POST: redirect to capture for a typed/selected account number."""
    acc = (request.form.get("account_number") or "").strip()
    if not acc:
        flash("Please enter an account number.", "error")
        return redirect(url_for("admin_bp.muni_accounts"))
    row = db.session.execute(
        text("SELECT 1 FROM bil_muni_account WHERE account_number = :acc"),
        {"acc": acc}
    ).fetchone()
    if not row:
        flash(f"Account {acc} not found.", "error")
        return redirect(url_for("admin_bp.muni_accounts"))
    return redirect(url_for("admin_bp.muni_capture", account_number=acc))

# --- Account picker ---
@admin_bp.route("/billing/muni", methods=["GET"])
def muni_accounts():
    q = (request.args.get("q") or "").strip()
    params = {}
    base_sql = """
        SELECT a.account_number, o.name AS owner_name,
               a.muni_water_meter_no, a.muni_elec_meter_no
        FROM bil_muni_account a
        LEFT JOIN ref_muni_owner o ON o.id = a.owner_id
    """
    if q:
        base_sql += " WHERE a.account_number LIKE :q OR o.name LIKE :q"
        params["q"] = f"%{q}%"
    base_sql += " ORDER BY a.account_number"
    accounts = db.session.execute(text(base_sql), params).mappings().all()
    all_nums = db.session.execute(
        text("SELECT account_number FROM bil_muni_account ORDER BY account_number")
    ).scalars().all()
    return render_template("admin/billing/muni/index.html", accounts=accounts, q=q, all_nums=all_nums)

# --- Capture & Ledger ---

def _get_aid(accno: str):
    r = db.session.execute(text("SELECT id FROM bil_muni_account WHERE account_number=:a"), {"a": accno}).fetchone()
    return r[0] if r else None

# --- Export bundle (ZIP) ---
def _rows_to_csv(rows, headers=None):
    import csv, io
    s = io.StringIO()
    hdrs = list(rows[0].keys()) if rows else (headers or [])
    w = csv.DictWriter(s, fieldnames=hdrs, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(dict(r))
    return s.getvalue()

# --- Legacy redirect so old links stop showing the old screen ---
@admin_bp.route("/billing/export")
def legacy_export_redirect():
    return redirect(url_for("admin_bp.muni_accounts"), code=301)


@admin_bp.route("/billing/debug_routes")
def _debug_routes():
    return "<pre>" + "\n".join(sorted(str(r) for r in current_app.url_map.iter_rules())) + "</pre>"


PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
# ---------- HUB PAGE ----------

# ---------- ALL ACCOUNTS ZIP (you already have similar) ----------
@admin_bp.route("/billing/muni/export/all.zip", methods=["GET"], endpoint="muni_export_all")
def muni_export_all():
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # accounts
        rows = db.session.execute(text("""
            SELECT a.account_number, o.name AS owner_name, a.email_electric,
                   a.muni_water_meter_no, a.muni_water_ref,
                   a.muni_elec_meter_no,  a.muni_elec_ref
            FROM bil_muni_account a
            LEFT JOIN ref_muni_owner o ON o.id=a.owner_id
            ORDER BY a.account_number
        """)).mappings().all()
        zf.writestr("accounts.csv", _rows_to_csv(rows, [
            "account_number","owner_name","email_electric",
            "muni_water_meter_no","muni_water_ref","muni_elec_meter_no","muni_elec_ref"
        ]))
        # monthly totals
        rows = db.session.execute(text("""
            SELECT a.account_number, t.period, t.balance, t.due, t.paid, t.arrears
            FROM bil_muni_cycle_totals t
            JOIN bil_muni_account a ON a.id=t.account_id
            ORDER BY a.account_number, t.period
        """)).mappings().all()
        zf.writestr("cycle_totals.csv", _rows_to_csv(rows, [
            "account_number","period","balance","due","paid","arrears"
        ]))
        # metsoa
        rows = db.session.execute(text("""
            SELECT a.account_number, m.period, m.metsoa_due
            FROM bil_metsoa_cycle m
            JOIN bil_muni_account a ON a.id=m.account_id
            ORDER BY a.account_number, m.period
        """)).mappings().all()
        zf.writestr("metsoa_cycle.csv", _rows_to_csv(rows, [
            "account_number","period","metsoa_due"
        ]))
        # views
        rows = db.session.execute(text("""
            SELECT account_number, period, balance, due, paid, arrears
            FROM v_admin_muni_ledger
            ORDER BY account_number, period
        """)).mappings().all()
        zf.writestr("ledger_view.csv", _rows_to_csv(rows, [
            "account_number","period","balance","due","paid","arrears"
        ]))
        rows = db.session.execute(text("""
            SELECT account_number, period, system_due, metro_due, diff
            FROM v_muni_due_vs_metsoa
            ORDER BY account_number, period
        """)).mappings().all()
        zf.writestr("recon_view.csv", _rows_to_csv(rows, [
            "account_number","period","system_due","metro_due","diff"
        ]))
    zbuf.seek(0)
    return send_file(zbuf, mimetype="application/zip", as_attachment=True,
                     download_name="muni_export_all.zip")

# ---------- PER-ACCOUNT ZIP ----------
@admin_bp.route("/billing/muni/export/account/<account_number>.zip", methods=["GET"], endpoint="muni_export_account")
def muni_export_account(account_number):
    # ensure exists
    row = db.session.execute(
        text("SELECT id FROM bil_muni_account WHERE account_number=:a"),
        {"a": account_number}
    ).fetchone()
    if not row: abort(404)
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # ledger (captured)
        rows = db.session.execute(text("""
            SELECT period, balance, due, paid, arrears
            FROM v_admin_muni_ledger
            WHERE account_number=:a
            ORDER BY period
        """), {"a": account_number}).mappings().all()
        zf.writestr(f"{account_number}_ledger.csv",
                    _rows_to_csv(rows, ["period","balance","due","paid","arrears"]))
        # recon
        rows = db.session.execute(text("""
            SELECT period, system_due, metro_due, diff
            FROM v_muni_due_vs_metsoa
            WHERE account_number=:a
            ORDER BY period
        """), {"a": account_number}).mappings().all()
        zf.writestr(f"{account_number}_recon.csv",
                    _rows_to_csv(rows, ["period","system_due","metro_due","diff"]))
    zbuf.seek(0)
    return send_file(zbuf, mimetype="application/zip", as_attachment=True,
                     download_name=f"{account_number}_muni_export.zip")

# ---------- DATE-RANGE ZIP (YYYY-MM to YYYY-MM) ----------
@admin_bp.route("/billing/muni/export/range.zip", methods=["GET"], endpoint="muni_export_range")
def muni_export_range():
    start = request.args.get("start","").strip()
    end   = request.args.get("end","").strip()
    if not (PERIOD_RE.match(start) and PERIOD_RE.match(end)):
        abort(400, "start/end must be YYYY-MM")
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        rows = db.session.execute(text("""
            SELECT account_number, period, balance, due, paid, arrears
            FROM v_admin_muni_ledger
            WHERE period BETWEEN :s AND :e
            ORDER BY account_number, period
        """), {"s": start, "e": end}).mappings().all()
        zf.writestr(f"ledger_{start}_to_{end}.csv",
                    _rows_to_csv(rows, ["account_number","period","balance","due","paid","arrears"]))
        rows = db.session.execute(text("""
            SELECT account_number, period, system_due, metro_due, diff
            FROM v_muni_due_vs_metsoa
            WHERE period BETWEEN :s AND :e
            ORDER BY account_number, period
        """), {"s": start, "e": end}).mappings().all()
        zf.writestr(f"recon_{start}_to_{end}.csv",
                    _rows_to_csv(rows, ["account_number","period","system_due","metro_due","diff"]))
    zbuf.seek(0)
    return send_file(zbuf, mimetype="application/zip", as_attachment=True,
                     download_name=f"muni_export_{start}_to_{end}.zip")

# ---------- PRINT-FRIENDLY LEDGER (browser print) ----------
@admin_bp.route("/billing/muni/print/ledger/<account_number>", methods=["GET"], endpoint="muni_print_ledger")
def muni_print_ledger(account_number):
    info = db.session.execute(text("""
        SELECT a.account_number, o.name AS owner_name
        FROM bil_muni_account a LEFT JOIN ref_muni_owner o ON o.id=a.owner_id
        WHERE a.account_number=:a
    """), {"a": account_number}).mappings().first()
    if not info: abort(404)
    rows = db.session.execute(text("""
        SELECT period, balance, due, paid, arrears
        FROM v_admin_muni_ledger
        WHERE account_number=:a
        ORDER BY period
    """), {"a": account_number}).mappings().all()
    recon = db.session.execute(text("""
        SELECT period, system_due, metro_due, diff
        FROM v_muni_due_vs_metsoa
        WHERE account_number=:a
        ORDER BY period
    """), {"a": account_number}).mappings().all()
    return render_template("admin/billing/muni/print_ledger.html",
                           account=info, rows=rows, recon=recon)

# ---------- OPTIONAL: PDF (WeasyPrint) ----------
@admin_bp.route("/billing/muni/export/ledger/<account_number>.pdf", methods=["GET"], endpoint="muni_export_ledger_pdf")
def muni_export_ledger_pdf(account_number):
    try:
        from weasyprint import HTML  # pip install weasyprint
    except Exception:
        abort(501, "PDF export not enabled on server")
    html = muni_print_ledger(account_number)
    if hasattr(html, "data"):  # if using flask Response from render_template
        html_str = html.data.decode("utf-8")
    else:
        html_str = html  # if you refactor to return string
    pdf = HTML(string=html_str, base_url=request.base_url).write_pdf()
    return send_file(io.BytesIO(pdf), mimetype="application/pdf", as_attachment=True,
                     download_name=f"{account_number}_ledger.pdf")

# app/admin/billing/routes.py


# ... your imports / admin_bp / db setup

def _to_decimal(val):
    if val is None: return None
    s = str(val).strip()
    if s == "": return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None

@admin_bp.route("/billing/muni/exports", methods=["GET"], endpoint="muni_exports_hub")
def muni_exports_hub():
    back = request.args.get("back")  # optional account number to return to capture
    accounts = db.session.execute(
        text("SELECT account_number FROM bil_muni_account ORDER BY account_number")
    ).scalars().all()
    return render_template("admin/billing/muni/exports.html", accounts=accounts, back=back)

def _to_number(val):
    """Return float or None from form input."""
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        # use float for sqlite param binding simplicity
        return float(Decimal(s))
    except (InvalidOperation, ValueError):
        return None

@admin_bp.route("/billing/muni/<account_number>/capture", methods=["GET", "POST"], endpoint="muni_capture")
def muni_capture(account_number):
    # Ensure account exists
    acc = db.session.execute(
        text("SELECT id, account_number FROM bil_muni_account WHERE account_number=:a"),
        {"a": account_number}
    ).mappings().first()
    if not acc:
        abort(404, "Unknown account")

    if request.method == "POST":
        kind   = (request.form.get("form_kind") or "").strip()
        period = (request.form.get("period") or "").strip()  # from <input type="month"> e.g. 2025-08

        if not period:
            flash("Period is required (YYYY-MM).", "error")
            return redirect(url_for("admin_bp.muni_capture", account_number=account_number))

        try:
            if kind == "totals":
                balance = _to_number(request.form.get("balance"))
                due     = _to_number(request.form.get("due"))
                paid    = _to_number(request.form.get("paid"))
                arrears = _to_number(request.form.get("arrears"))

                # 1) try UPDATE
                res = db.session.execute(text("""
                    UPDATE bil_muni_cycle_totals
                       SET balance=:b, due=:d, paid=:pa, arrears=:ar
                     WHERE account_id=:id AND period=:p
                """), {"id": acc["id"], "p": period, "b": balance, "d": due, "pa": paid, "ar": arrears})

                # 2) if no row updated, INSERT
                if res.rowcount == 0:
                    db.session.execute(text("""
                        INSERT INTO bil_muni_cycle_totals
                            (account_id, period, balance, due, paid, arrears)
                        VALUES (:id, :p, :b, :d, :pa, :ar)
                    """), {"id": acc["id"], "p": period, "b": balance, "d": due, "pa": paid, "ar": arrears})

                db.session.commit()
                flash(f"Monthly totals saved for {period}.", "success")

            elif kind == "metsoa":
                metsoa_due = _to_number(request.form.get("metsoa_due"))

                res = db.session.execute(text("""
                    UPDATE bil_metsoa_cycle
                       SET metsoa_due=:m
                     WHERE account_id=:id AND period=:p
                """), {"id": acc["id"], "p": period, "m": metsoa_due})

                if res.rowcount == 0:
                    db.session.execute(text("""
                        INSERT INTO bil_metsoa_cycle (account_id, period, metsoa_due)
                        VALUES (:id, :p, :m)
                    """), {"id": acc["id"], "p": period, "m": metsoa_due})

                db.session.commit()
                flash(f"METSOA saved for {period}.", "success")

        except Exception as e:
            db.session.rollback()
            # show a friendly message; dev console will have the traceback
            flash("Save failed. Check server log for details.", "error")
            # OPTIONAL: add a temporary print to help locate issues
            print("muni_capture SAVE error:", repr(e))

        return redirect(url_for("admin_bp.muni_capture", account_number=account_number))

    # GET — hydrate page
    current_period = request.args.get("period")  # optional preselect

    ledger = db.session.execute(text("""
        SELECT period, balance, due, paid, arrears
          FROM v_admin_muni_ledger
         WHERE account_number=:a
         ORDER BY period DESC
    """), {"a": account_number}).mappings().all()

    recon = db.session.execute(text("""
        SELECT period, system_due, metro_due, diff
          FROM v_muni_due_vs_metsoa
         WHERE account_number=:a
         ORDER BY period DESC
    """), {"a": account_number}).mappings().all()

    return render_template(
        "admin/billing/muni/capture.html",
        account_number=account_number,
        current_period=current_period,
        ledger=ledger,
        recon=recon
    )

# ---------- TENANT ROUTES (list/search, create, edit) ----------

@admin_bp.post("/billing/tenants/new")
def tenants_create():
    form = request.form
    name = (form.get("name") or "").strip()
    if not name:
        flash("Name is required", "error")
        return redirect(request.referrer or url_for("admin_bp.tenants_new"))

    t = BilTenant(
        name=name,
        unit_name=(form.get("unit_name") or "").strip() or None,
        metro_account_no=(form.get("metro_account_no") or "").strip() or None,
        rent_includes_metro=1 if (form.get("rent_includes_metro") == "on") else 0,
        email=(form.get("email") or "").strip() or None,
        phone=(form.get("phone") or "").strip() or None,
        notes=(form.get("notes") or "").strip() or None,
    )
    db.session.add(t)
    db.session.commit()

    nxt = form.get("next") or url_for("admin_bp.tenant_index")
    flash("Tenant created", "success")
    return redirect(nxt)

@admin_bp.get("/billing/tenants/<int:tenant_id>/edit")
def tenants_edit(tenant_id):
    t = db.session.get(BilTenant, tenant_id)
    if not t:
        flash("Tenant not found", "error")
        return redirect(url_for("admin_bp.tenant_index"))
    nxt = request.args.get("next") or url_for("admin_bp.tenant_index")
    return render_template("billing/tenant/form.html", mode="edit", tenant=t, nxt=nxt)

@admin_bp.post("/billing/tenants/<int:tenant_id>/edit")
def tenants_update(tenant_id):
    t = db.session.get(BilTenant, tenant_id)
    if not t:
        flash("Tenant not found", "error")
        return redirect(url_for("admin_bp.tenant_index"))

    form = request.form
    name = (form.get("name") or "").strip()
    if not name:
        flash("Name is required", "error")
        return redirect(request.referrer or url_for("admin_bp.tenant_edit", tenant_id=tenant_id))

    t.name = name
    t.unit_name = (form.get("unit_name") or "").strip() or None
    t.metro_account_no = (form.get("metro_account_no") or "").strip() or None
    t.rent_includes_metro = 1 if (form.get("rent_includes_metro") == "on") else 0
    t.email = (form.get("email") or "").strip() or None
    t.phone = (form.get("phone") or "").strip() or None
    t.notes = (form.get("notes") or "").strip() or None

    db.session.commit()
    nxt = form.get("next") or url_for("admin_bp.tenant_index")
    flash("Tenant updated", "success")
    return redirect(nxt)
# ---------- /TENANT ROUTES ----------


# 1) Ensure at least one unit exists (so the form can work)
@admin_bp.get("/billing/units/ensure_one", endpoint="units_ensure_one")
def units_ensure_one():
    u = BilSectionalUnit.query.first()
    if not u:
        u = BilSectionalUnit(name="Unit A-101")
        db.session.add(u)
        db.session.commit()
    return f"OK: unit id={u.id} name={u.name}"
'''
# 2) List tenants (plain HTML)
@admin_bp.get("/billing/tenants/smoke", endpoint="tenants_smoke")
def tenants_smoke():
    q = (request.args.get("q") or "").strip()
    query = BilTenant.query
    if q:
        like = f"%{q}%"
        from sqlalchemy import or_
        query = query.filter(or_(
            BilTenant.name.ilike(like),
            BilTenant.email.ilike(like),
            BilTenant.phone.ilike(like),
            BilTenant.metro_account_no.ilike(like),
        ))
    tenants = query.order_by(BilTenant.name.asc()).all()
    units = BilSectionalUnit.query.order_by(BilSectionalUnit.name.asc()).all()

    html = """
    <h1>Tenants SMOKE</h1>
    <p>count={{ tenants|length }}</p>
    <form method="get">
      <input name="q" value="{{ q }}" placeholder="search">
      <button>Search</button>
    </form>

    <ul>
    {% for t in tenants %}
      <li>#{{ t.id }} — {{ t.name }}
          — unit: {{ t.sectional_unit.name if t.sectional_unit else "None" }}
          — metro: {{ t.metro_account_no or "—" }}
      </li>
    {% else %}
      <li>No tenants</li>
    {% endfor %}
    </ul>

    <hr>
    <h2>Create quick tenant</h2>
    <form method="post" action="{{ url_for('admin_bp.tenants_smoke_create') }}">
      <label>Name*</label>
      <input name="name" required>
      <label>Unit*</label>
      <select name="sectional_unit_id" required>
        {% for u in units %}
          <option value="{{ u.id }}">{{ u.name }}</option>
        {% endfor %}
      </select>
      <label>Metro</label>
      <input name="metro_account_no">
      <label>Email</label>
      <input name="email" type="email">
      <label>Phone</label>
      <input name="phone">
      <label>Rent includes metro</label>
      <input type="checkbox" name="rent_includes_metro" checked>
      <button>Create</button>
    </form>
    """
    return render_template_string(html, tenants=tenants, units=units, q=q)


# 3) Create tenant (plain POST)
@admin_bp.post("/billing/tenants/smoke", endpoint="tenants_smoke_create")
def tenants_smoke_create():
    name = (request.form.get("name") or "").strip()
    suid = request.form.get("sectional_unit_id")
    if not name or not suid:
        abort(400, "name and sectional_unit_id required")
    t = BilTenant(
        name=name,
        sectional_unit_id=int(suid),
        metro_account_no=(request.form.get("metro_account_no") or None),
        rent_includes_metro=1 if request.form.get("rent_includes_metro") == "on" else 0,
        email=(request.form.get("email") or None),
        phone=(request.form.get("phone") or None),
        notes=None,
    )
    db.session.add(t)
    db.session.commit()
    return redirect(url_for("admin_bp.tenants_smoke"))
'''
# --- Tenants CRUD (simple, no Flask-WTF) ---

@admin_bp.route("/billing/occupants/<int:tenant_id>/edit", methods=["GET", "POST"], endpoint="billing_occupants_edit")
def billing_occupants_edit(tenant_id: int):
    t = BilTenant.query.get_or_404(tenant_id)
    units = BilSectionalUnit.query.order_by(BilSectionalUnit.name.asc()).all()
    if request.method == "POST":
        t.name = (request.form.get("name") or "").strip()
        t.sectional_unit_id = int(request.form.get("sectional_unit_id"))
        t.metro_account_no = (request.form.get("metro_account_no") or None)
        t.rent_includes_metro = 1 if request.form.get("rent_includes_metro") == "on" else 0
        t.email = (request.form.get("email") or None)
        t.phone = (request.form.get("phone") or None)
        t.notes = (request.form.get("notes") or None)
        db.session.commit()
        return redirect(url_for("admin_bp.billing_occupants_index"))
    return render_template("admin/billing/occupants/form.html", tenant=t, units=units)

@admin_bp.post("/billing/toccupants/<int:tenant_id>/delete", endpoint="billing_occupants_delete")
def billing_occupants_delete(tenant_id: int):
    t = BilTenant.query.get_or_404(tenant_id)
    db.session.delete(t)
    db.session.commit()
    return redirect(url_for("admin_bp.billing_occupants_index"))

# app/admin/billing/routes.py  (only showing differences)

'''
@admin_bp.get("/billing/statement/<int:tenant_id>.pdf", endpoint="billing_statement_pdf")
def billing_statement_pdf(tenant_id: int):
    # TODO: build your data dict for the template
    # e.g. data = build_statement_context(tenant_id)
    data = {"tenant_id": tenant_id}  # placeholder

    html = render_template("admin/billing/statement.html", data=data)

    # Generate PDF in-memory
    pdf_bytes = HTML(string=html, base_url=request.url_root).write_pdf()

    # Return as a download
    return current_app.response_class(
        pdf_bytes,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="statement_{tenant_id}.pdf"'
        },
    )
'''
# app/admin/billing/routes.py


def active_lease_filter():
    today = date.today()
    return and_(BilLease.lease_start <= today,
                or_(BilLease.lease_end.is_(None), BilLease.lease_end >= today))

from datetime import date


'''
'''

def _active_lease_filter():
    today = date.today()
    return and_(BilLease.lease_start <= today,
                or_(BilLease.lease_end.is_(None), BilLease.lease_end >= today))

# app/admin/billing/routes.py

#from app.models.billing import BilSectionalUnit, BilTenant, BilMeter  # and BilLease later

admin_bp = Blueprint("admin_bp", __name__, url_prefix="/admin")

# ---------- Data Entry hub ----------
@admin_bp.get("/billing/data")
def billing_data():
    return render_template("admin/billing/data/index.html")

# ---------- Units ----------
@admin_bp.get("/billing/units")
def units_index():
    # TODO: compute occupied/free counts if you want badges
    units = BilSectionalUnit.query.order_by(BilSectionalUnit.name.asc()).all()
    return render_template("admin/billing/units/index.html", units=units)

@admin_bp.route("/billing/units/new", methods=["GET", "POST"])
def units_new():
    if request.method == "POST":
        # TODO: create & commit a new BilSectionalUnit from form fields
        # flash("Unit added.", "success")
        return redirect(url_for("admin_bp.units_index"))
    return render_template("admin/billing/units/new.html")

# ---------- Occupants (Tenants) ----------
@admin_bp.post("/billing/occupants/new", endpoint="billing_occupants_create")
def billing_occupants_create():
    name    = (request.form.get("name") or "").strip()
    unit_id = request.form.get("sectional_unit_id", type=int)
    email   = (request.form.get("email") or "").strip() or None
    phone   = (request.form.get("phone") or "").strip() or None
    metro   = (request.form.get("metro_account_no") or "").strip() or None
    rim     = 1 if request.form.get("rent_includes_metro") else 0
    start   = (request.form.get("lease_start") or "").strip()
    end     = (request.form.get("lease_end") or "").strip()

    if not name or not unit_id:
        flash("Full name and Sectional Unit are required.", "error")
        return redirect(url_for("admin_bp.billing_occupants_new"))

    # simple guard: reject if some tenant already claims that unit
    exists = db.session.query(BilTenant.id).filter(BilTenant.sectional_unit_id == unit_id).first()
    if exists:
        flash("That unit is already occupied. Please choose another unit.", "error")
        return redirect(url_for("admin_bp.billing_occupants_new"))

    try:
        t = BilTenant(
            name=name,
            sectional_unit_id=unit_id,
            email=email,
            phone=phone,
            metro_account_no=metro,
            rent_includes_metro=rim,
            notes=(request.form.get("notes") or None),
        )
        db.session.add(t)
        db.session.flush()  # get t.id

        if start:
            lease = BilLease(
                tenant_id=t.id,
                sectional_unit_id=unit_id,
                start_date=date.fromisoformat(start),
                end_date=(date.fromisoformat(end) if end else None),
                # other BilLease fields only if they exist in your schema:
                # rent_amount=..., day_of_month=..., notes=...
            )
            db.session.add(lease)

        db.session.commit()
        flash("Tenant created.", "success")
        return redirect(url_for("admin_bp.billing_occupants_index"))
    except Exception as e:
        db.session.rollback()
        flash(f"Could not create tenant: {e}", "error")
        return redirect(url_for("admin_bp.billing_occupants_new"))


# LIST
@admin_bp.get("/billing/occupants", endpoint="billing_occupants_index")
def billing_occupants_index():
    tenants = BilTenant.query.order_by(BilTenant.name.asc()).all()
    return render_template("admin/billing/occupants/index.html", tenants=tenants)



# NEW (GET+POST)
@admin_bp.get("/billing/occupants/new", endpoint="billing_occupants_new")
def billing_occupants_new():
    units = BilSectionalUnit.query.order_by(BilSectionalUnit.name.asc()).all()

    # mark occupied by existing tenants (simple fallback)
    occupied_ids = {
        sid for (sid,) in db.session.query(BilTenant.sectional_unit_id)
        .filter(BilTenant.sectional_unit_id.isnot(None)).all()
    }

    return render_template(
        "admin/billing/occupants/form.html",
        tenant=None,
        units=units,
        occupied_ids=occupied_ids,
    )

# (Optional) VIEW — simple, non-lease version
@admin_bp.get("/billing/occupants/<int:tenant_id>", endpoint="billing_occupants_view")
def billing_occupants_view(tenant_id):
    tenant = db.session.get(BilTenant, tenant_id)
    if not tenant:
        abort(404)
    return render_template("admin/billing/occupants/show.html", tenant=tenant)

