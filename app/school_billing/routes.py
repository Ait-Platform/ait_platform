from flask import Blueprint, render_template, request, redirect, url_for, flash, session, abort
from app import db
from flask import current_app
from datetime import datetime, timedelta
# Models (clean and complete)
from app.models.billing import (
    BilProperty, BilTenant, BilMeter, BilMeterReading,
    BilConsumption, BilTariff, BilSectionalUnit, 
    BilMeterFixedCharge, PropertyForm
    )
from flask_login import login_user, logout_user, login_required, current_user
from app.models.auth import User
from werkzeug.security import check_password_hash, generate_password_hash
import hashlib
from sqlalchemy import text
from flask import jsonify
import time
from app.auth.forms import RegisterForm, ManagerPropertyForm, TenancyForm
from app.school_billing.helpers import get_dashboard_data

billing_bp = Blueprint('billing_bp', __name__)

@billing_bp.route('/billing/about')
def billing_about():
    return render_template("school_billing/about.html")

@billing_bp.route("/dashboard/manager", methods=["GET", "POST"])
@login_required
def manager_dashboard():

    if current_user.role != "manager":
        flash("Access denied", "danger")
        return redirect(url_for("public_bp.welcome"))

    if request.method == "POST":
        # Create property + unit in one go
        prop = BilProperty(
            name=request.form["property_name"],
            address=request.form["address"],
            description=request.form.get("description"),
            manager_id=current_user.id
        )
        db.session.add(prop)
        db.session.flush()

        unit = BilSectionalUnit(
            property_id=prop.id,
            unit_number=request.form["unit_number"]
        )
        db.session.add(unit)
        db.session.commit()

        flash("Property and unit added successfully!", "success")
        return redirect(url_for("billing_bp.manager_dashboard"))

    # Normal GET → show dashboard data
    data = get_dashboard_data()

    return render_template("school_billing/manager_dashboard.html", data=data)

@billing_bp.route("/dashboard/tenant")
@login_required
def tenant_dashboard():
    # Only allow tenants
    if current_user.role != "tenant":
        flash("Access denied", "danger")
        return redirect(url_for("public_bp.welcome"))
    data = get_dashboard_data()
    return render_template("school_billing/tenant_dashboard.html", data=data)



@billing_bp.route("/dashboard/admin", methods=["GET", "POST"])
@login_required
def admin_dashboard():
    # Only allow admins
    if current_user.role != "admin":
        flash("Access denied", "danger")
        return redirect(url_for("public_bp.welcome"))

    if request.method == "POST":
        # Create property + unit in one go
        prop = BilProperty(
            name=request.form["property_name"],
            address=request.form["address"],
            description=request.form.get("description"),
            manager_id=current_user.id
        )
        db.session.add(prop)
        db.session.flush()

        unit = BilSectionalUnit(
            property_id=prop.id,
            unit_number=request.form["unit_number"]
        )
        db.session.add(unit)
        db.session.commit()

        flash("Property and unit added successfully!", "success")
        return redirect(url_for("billing_bp.manager_dashboard"))

    # Normal GET → show dashboard data
    data = get_dashboard_data()

    return render_template("school_billing/admin_dashboard.html", data=data)


def hash_password(password):
    return generate_password_hash(password)

def generate_month_list(n=6):
    today = datetime.today()
    months = []
    for i in range(n):
        month = (today.replace(day=1) - timedelta(days=30 * i)).strftime('%Y-%m')
        months.append(month)
    return sorted(set(months))

@billing_bp.route("/setup-property/<int:user_id>", methods=["GET", "POST"])
def setup_property(user_id):
    form = ManagerPropertyForm()
    manager = User.query.get_or_404(user_id)

    if form.validate_on_submit():
        property = BilProperty(
            name=form.property_name.data,
            address=form.address.data,
            unit_count=form.unit_count.data,
            manager_id=manager.id,
            property_type="external"
        )
        db.session.add(property)
        db.session.commit()
        return redirect(url_for("billing.manager_dashboard"))
    return render_template("setup_property.html", form=form)

def get_available_months():
    return db.session.execute(text("""
        SELECT DISTINCT strftime('%Y-%m', reading_date)
        FROM bil_meter_reading
        ORDER BY 1 DESC
    """)).scalars().all()

def get_latest_month_for_tenant(tenant_id):
    return db.session.execute(text("""
        SELECT strftime('%Y-%m', reading_date)
        FROM bil_meter_reading
        WHERE tenant_id = :tenant_id
        ORDER BY reading_date DESC
        LIMIT 1
    """), {"tenant_id": tenant_id}).scalar()


def delete_all_consumption():
    """Deletes all records from bil_consumption table."""
    db.session.execute(text("DELETE FROM bil_consumption"))
    db.session.commit()
    print("🗑️ bil_consumption table cleared.")

def delete_all_consumption_copy():
    """Deletes all records from bil_consumption_copy table."""
    db.session.execute(text("DELETE FROM bil_consumption_copy"))
    db.session.commit()
    print("🗑️ bil_consumption_copy table cleared.")

def get_billing_months(num_months=6):
    """
    Returns a list of billing months in 'YYYY-MM' format,
    going back from the current month.
    """
    today = datetime.today()
    months = []

    for i in range(num_months):
        month = today.replace(day=1) - timedelta(days=i*30)
        months.append(month.strftime("%Y-%m"))

    # Optional: remove duplicates and sort descending
    return sorted(set(months), reverse=True)

@billing_bp.route('/admin/wipe_data', methods=['POST'])
@login_required
def wipe_tables():
    delete_all_consumption()
    delete_all_consumption_copy()
    flash("Consumption tables wiped clean.", "warning")
    return redirect(url_for("billing.admin_dashboard"))
 

# 🌐 Onboarding Route — stays in app/routes.py
@billing_bp.route('/onboard-property', methods=['GET', 'POST'])
@login_required
def onboard_property():
    if current_user.role not in ['external_manager', 'admin']:
        abort(403)

    form = PropertyForm()

    if form.validate_on_submit():
        new_property = BilProperty(
            name=form.name.data,
            location=form.location.data,
            type=form.type.data,
            is_external=True,
            managed_by_user_id=current_user.id
        )

        try:
            db.session.add(new_property)
            db.session.commit()
            flash("✅ Property onboarded successfully!", "success")
            return redirect(url_for("define_units", property_id=new_property.id))
        except Exception as e:
            db.session.rollback()
            print(f"💥 Property creation failed: {e}")
            flash("There was an error onboarding the property.", "danger")

    return render_template("onboard_property.html", form=form)

def generate_consumption_records_from_readings(month):
    """
    Regenerates consumption records for the given month by:
    - Deleting previous BilConsumption records for the month
    - Creating one consumption entry per meter based on consecutive readings
    """
    # 🚫 Clear existing records for the month
    BilConsumption.query.filter_by(month=month).delete()
    db.session.commit()

    records = []
    meters = BilMeter.query.all()

    if not meters:
        print("No meters found — skipping generation.")
        return records

    for meter in meters:
        # 🔍 Fetch all readings ordered by date for this meter
        readings = BilMeterReading.query.filter_by(meter_id=meter.id).order_by(BilMeterReading.reading_date).all()

        # ✅ Look for the first valid reading pair in the target month
        for i in range(1, len(readings)):
            prev = readings[i - 1]
            curr = readings[i]

            if curr.reading_date.strftime('%Y-%m') != month:
                continue

            try:
                days = (curr.reading_date - prev.reading_date).days
                usage = curr.reading_value - prev.reading_value

                record = BilConsumption(
                    meter_id=meter.id,
                    meter_number=meter.meter_number,
                    last_date=prev.reading_date,
                    new_date=curr.reading_date,
                    last_read=prev.reading_value,
                    new_read=curr.reading_value,
                    days=days,
                    consumption=int(round(usage)),
                    month=month
                )
                records.append(record)
            except Exception as e:
                print(f"⚠️ Error processing meter {meter.meter_number}: {e}")

            break  # Only generate one record per meter per month

    # 💾 Commit all new records
    if records:
        db.session.add_all(records)
        db.session.commit()
        print(f"✅ {len(records)} consumption records generated for {month}")
    else:
        print(f"⚠️ No valid consumption records generated for {month}")

    return records

def build_meter_charge_block(meter_id, month):
    rows = []

    # Get consumption
    consumption = BilConsumption.query.filter_by(
        meter_id=meter_id, month=month
    ).first()
    if not consumption:
        return rows  # Skip if no data

    # Tiered water blocks
    tariff_blocks = BilTariff.query.filter_by(
        utility_type="water"
    ).order_by(BilTariff.block_start.asc()).all()
    blocks = [{"start": t.block_start, "end": t.block_end, "rate": t.rate} for t in tariff_blocks]

    # Tiered logic
    def calculate_tiered_due(consumption_value, blocks):
        due = 0
        for block in blocks:
            if consumption_value > block["start"]:
                used = min(consumption_value, block["end"]) - block["start"]
                due += used * block["rate"]
        return round(due, 2)

    tiered_due = calculate_tiered_due(consumption.consumption, blocks)

    # Fixed charges
    fixed_charges = BilMeterFixedCharge.query.filter_by(
        meter_id=meter_id, month=month
    ).all()
    fixed_rows = []
    fixed_total = 0
    for fc in fixed_charges:
        fixed_rows.append({
            "description": fc.description,
            "amount": round(fc.amount, 2),
            "cons": fc.cons,
            "rate": fc.rate
        })
        fixed_total += fc.amount

    # WS Header
    rows.append({
        "meter_number": f"Water & Sanitation — {consumption.meter_number}",
        "last_date": "",
        "last_read": "",
        "new_date": "",
        "new_read": "",
        "days": "",
        "consumption": "",
        "rate": "",
        "due": ""
    })

    # Tiered row
    rows.append({
        "meter_number": f"{consumption.meter_number} — Tiered Water",
        "last_date": consumption.last_date.strftime('%Y-%m-%d'),
        "last_read": consumption.last_read,
        "new_date": consumption.new_date.strftime('%Y-%m-%d'),
        "new_read": consumption.new_read,
        "days": consumption.days,
        "consumption": consumption.consumption,
        "rate": "Tiered",
        "due": tiered_due
    })

    # Fixed charge rows
    for fr in fixed_rows:
        rows.append({
            "meter_number": f"{consumption.meter_number} — {fr['description']}",
            "last_date": "",
            "last_read": "",
            "new_date": "",
            "new_read": "",
            "days": "",
            "consumption": fr["cons"],
            "rate": fr["rate"],
            "due": fr["amount"]
        })

    # Subtotal
    rows.append({
        "meter_number": f"{consumption.meter_number} — WS Subtotal",
        "last_date": "",
        "last_read": "",
        "new_date": "",
        "new_read": "",
        "days": "",
        "consumption": "",
        "rate": "",
        "due": round(tiered_due + fixed_total, 2)
    })

    return rows

def calculate_ws_sd_total(meter_id, month):
    """
    Returns subtotal due for Water & Sanitation + SD charges for one meter.
    Used for Page 1 summary row.
    """

    # Fetch tiered water consumption
    consumption = BilConsumption.query.filter_by(
        meter_id=meter_id, month=month
    ).first()

    # Early exit if no data
    if not consumption:
        return {
            "meter_number": f"Meter ID {meter_id}",
            "due": 0
        }

    # Tiered WS tariff blocks
    tariff_blocks = BilTariff.query.filter_by(
        utility_type="water"
    ).order_by(BilTariff.block_start.asc()).all()

    blocks = [{"start": t.block_start, "end": t.block_end, "rate": t.rate} for t in tariff_blocks]

    # Calculate tiered water due
    def calculate_tiered_due(cons_value, blocks):
        due = 0
        for block in blocks:
            if cons_value > block["start"]:
                used = min(cons_value, block["end"]) - block["start"]
                due += used * block["rate"]
        return round(due, 2)

    tiered_due = calculate_tiered_due(consumption.consumption, blocks)

    # Fetch fixed charges
    fixed_charges = BilMeterFixedCharge.query.filter_by(
        meter_id=meter_id, month=month
    ).all()

    fixed_total = sum([fc.amount for fc in fixed_charges])

    # Total subtotal
    total = round(tiered_due + fixed_total, 2)

    return {
        "meter_number": consumption.meter_number,
        "due": total
    }

def build_ws_sd_rows_for_meter(meter_id):
    # Step 1: Pull the meter object
    meter = BilMeter.query.get(meter_id)
    if not meter:
        return [], 0  # Handle missing meter

    # Step 2: Pull consumption record using meter_id
    record = BilConsumption.query.filter_by(meter_id=meter_id).first()
    if not record:
        return [], 0  # Handle missing data

    consumption = record.consumption
    days = record.days

    # Step 3: Tiered WS breakdown
    ws_rows, ws_total = build_ws_breakdown(consumption)

    # Step 4: Tiered SD breakdown
    sd_rows, sd_total = build_sd_breakdown(consumption, days)

    # Step 5: Fixed & Sundry Charges
    fee_rows = build_water_fee_block(meter.id)  # or pass `meter_id` directly if updated

    fixed_total = round(sum(row["due"] for row in fee_rows), 2)

    # Step 6: Total Due
    total_due = round(ws_total + sd_total + fixed_total, 2)

    # Step 7: Assemble Page 2 Rows
    rows_page2 = []

    meter_number = meter.meter_number  # for display only
    rows_page2.append({"meter_number": meter_number, "consumption": "WS Breakdown", "rate": "", "due": ""})
    rows_page2.extend(ws_rows)

    rows_page2.append({"meter_number": meter_number, "consumption": "SD Breakdown", "rate": "", "due": ""})
    rows_page2.extend(sd_rows)

    rows_page2.append({"meter_number": meter_number, "consumption": "Fixed & Sundry", "rate": "", "due": ""})
    rows_page2.extend(fee_rows)

    rows_page2.append({
        "meter_number": meter_number,
        "consumption": "Total Metro Charges",
        "rate": "",
        "due": total_due
    })

    return rows_page2, total_due

def build_ws_sd_subtotals():
    """
    Returns structured WS, SD, and total rows for Page 1.
    Total returned separately for metro accumulation.
    """
    # Mock values for demo
    ws = round(74.04, 2)
    sd = round(53.40, 2)
    total = round(ws + sd, 2)

    rows = [
        {
            "meter_number":"WS Sub-Total",
            "last_date": "", "new_date": "",
            "last_read": "", "new_read": "", "days": "",
            "rate": "", "consumption": "",
            "due": ws
        },
        {
            "meter_number":"SD Sub-Total",
            "last_date": "", "new_date": "",
            "last_read": "", "new_read": "", "days": "",
            "rate": "", "consumption": "",
            "due": sd
        },
        {
            "meter_number":"Cost of Water",
            "last_date": "", "new_date": "",
            "last_read": "", "new_read": "", "days": "",
            "rate": "", "consumption": "",
            "due": total
        }
    ]

    return rows, ws, sd, total
    
def build_ws_breakdown(consumption, days, month, meter_id):
    ws_tiers = get_tariff_for_ws(meter_id, month)  # Pull from DB or tariff config

    breakdown = []
    total_due = 0.0
    remaining = consumption

    for tier in ws_tiers:
        max_alloc = round(tier["multiplier"] * days, 3)
        used = min(remaining, max_alloc)
        due = round(used * tier["rate"], 2)

        breakdown.append({
            "desc": f"WS Tier @ R{tier['rate']}",
            "cons": used,
            "rate": tier["rate"],
            "due": due
        })

        total_due += due
        remaining -= used
        if remaining <= 0:
            break

    return breakdown, round(total_due, 2)

def build_sd_breakdown(consumption, days, month, meter_id):
    sd_tiers = get_tariff_for_sd(meter_id, month)  # Pull SD structure from DB

    breakdown = []
    total_due = 0.0
    remaining = consumption

    for tier in sd_tiers:
        max_alloc = round(tier["multiplier"] * days, 3)
        used = min(remaining, max_alloc)
        due = round(used * tier["rate"], 2)

        breakdown.append({
            "desc": f"SD Tier @ R{tier['rate']}",
            "cons": used,
            "rate": tier["rate"],
            "due": due
        })

        total_due += due
        remaining -= used
        if remaining <= 0:
            break

    return breakdown, round(total_due, 2)

def build_water_fee_block(r, month):
    meter_id = r.meter_number
    consumption = r.consumption

    fixed_charge = get_fixed_charge_for_meter(meter_id, month)
    water_loss_levy = get_water_loss_levy(meter_id, month)
    sundry_ws = get_sundry_ws(consumption)
    sundry_sd = get_sundry_sd(consumption)
    #refuse_charge = REFUSE_MAP.get(meter_id, {}).get("rate", 0)
    #mgmt_fee = MGMT_FEE_MAP.get(meter_id, 0)

    return [
        {"desc": "Fixed Water Charge", "due": round(fixed_charge, 2)},
        {"desc": "Water Loss Levy", "due": round(water_loss_levy, 2)},
        {"desc": "Sundry WS", "due": round(sundry_ws, 2)},
        {"desc": "Sundry SD", "due": round(sundry_sd, 2)},
        #{"desc": "Refuse Bin", "due": round(refuse_charge, 2)},
        #{"desc": "Management Fee", "due": round(mgmt_fee, 2)}
    ]

def build_raw_water_row(meter_id, month):
    """
    Returns a single row for the given water meter using only consumption table data.
    'Rate' and 'Due' fields are intentionally left blank.
    Used for layout alignment with electrical block on Page 1.
    """

    cons = BilConsumption.query.filter_by(
        meter_id=meter_id, month=month
    ).first()
    if not cons:
        return []

    return [{
        "meter_number": str(cons.meter_id),
        "code": cons.meter_number,
        "last_date": cons.last_date.strftime("%Y/%m/%d"),
        "last_read": cons.last_read,
        "new_date": cons.new_date.strftime("%Y/%m/%d"),
        "new_read": cons.new_read,
        "days": cons.days,
        "consumption": cons.consumption,
        "rate": "",
        "due": ""
    }]

def build_electrical_rows(tenant_id, month):
    """
    Returns charge rows + subtotal for electricity, pulled from BilConsumption and BilTariff.
    """
    rows = []
    total_due = 0

    # Get linked meters
    linked_meter_ids = [
        m.id for m in BilMeter.query.filter_by(sectional_unit_id=tenant_id).all()
    ]

    # Preload meter types
    meter_types = {
        m.id: m.utility_type
        for m in BilMeter.query.filter(BilMeter.id.in_(linked_meter_ids)).all()
    }

    # Get consumption records
    records = BilConsumption.query.filter_by(month=month).all()
    tenant_records = [r for r in records if r.meter_id in linked_meter_ids]

    # Get electricity rate
    rate_obj = BilTariff.query.filter_by(utility_type="electricity").first()
    rate = rate_obj.rate if rate_obj else 0

    # Build rows
    for r in tenant_records:
        if meter_types.get(r.meter_id) == "electricity":
            due = round(r.consumption * rate, 2)
            total_due += due

            rows.append({
                "meter_number": r.meter_number,
                "last_date": r.last_date.strftime('%Y-%m-%d'),
                "last_read": r.last_read,
                "new_date": r.new_date.strftime('%Y-%m-%d'),
                "new_read": r.new_read,
                "days": r.days,
                "consumption": r.consumption,
                "rate": rate,
                "due": due
            })

    # Add subtotal
    rows.append({
        "meter_number": "Electrical Sub-Total",
        "last_date": "", "last_read": "",
        "new_date": "", "new_read": "",
        "days": "", "consumption": "",
        "rate": "", "due": round(total_due, 2)
    })

    return rows

def get_tariff_for_ws(meter_id, month):
    # Placeholder: replace with DB query
    return [
        {"start": 0, "multiplier": 0.200, "rate": 34.87},
        {"start": 201, "multiplier": 0.833, "rate": 41.30},
        {"start": 834, "multiplier": 1.000, "rate": 55.04},
        {"start": 1001, "multiplier": 1.500, "rate": 84.87}
    ]

def get_tariff_for_sd(meter_id, month):
    return [
        {"start": 0, "multiplier": 0.200, "rate": 5.45},
        {"start": 201, "multiplier": 0.833, "rate": 9.20},
        {"start": 834, "multiplier": 1.000, "rate": 17.54},
        {"start": 1001, "multiplier": 1.500, "rate": 27.38}
    ]

def build_water_rows(tenant_id, month):
    """
    Returns:
      - Page 1 layout rows for water meters
      - Page 2 tiered blocks via meter_rows
      - Metro total due across all water meters
    """
    rows = []
 
    # 🔹 Get linked meters
    linked_meter_ids = [
        m.id for m in BilMeter.query.filter_by(sectional_unit_id=tenant_id).all()
    ]

    # 🔹 Preload meter types
    meter_types = {
        m.id: m.utility_type
        for m in BilMeter.query.filter(BilMeter.id.in_(linked_meter_ids)).all()
    }

    # 🔹 Get consumption records
    records = BilConsumption.query.filter_by(month=month).all()
    tenant_records = [r for r in records if r.meter_id in linked_meter_ids]



    # 🔁 Process water meters
    for r in tenant_records:
        if meter_types.get(r.meter_id) == "water":
            #print(f"Adding water rows for meter {r.meter_number}")
            meter_id = r.meter_id

            # 🔹 Step 1: Add actual consumption row
            rows.append({
                "meter_number": r.meter_number,
                "last_date": r.last_date.strftime('%Y-%m-%d'),
                "last_read": r.last_read,
                "new_date": r.new_date.strftime('%Y-%m-%d'),
                "new_read": r.new_read,
                "days": r.days,
                "consumption": r.consumption,
                "rate": "",
                "due": ""
            })
            #meter_id = r.meter_id
            # 🔹 Step 2: Add helper subtotal rows scoped per meter
            #rows += build_ws_sd_rows_for_meter(meter_id)
        

        rows_page2, total_due = build_ws_sd_rows_for_meter(meter_id)
        rows += rows_page2

    return rows

def add_water_subtotals_to_page1(rows, meter_number, cost_block):
    """
    Appends WS, SD, and total water cost rows to Page 1 layout.
    """
    rows.extend([
        {
            "meter_number": "",
            "code": f"W&S Cost For #{meter_number}",
            "last_date": "", "last_read": "",
            "new_date": "", "new_read": "",
            "days": "", "consumption": "",
            "rate": "", "due": cost_block["ws_total"]
        },
        {
            "meter_number": "",
            "code": f"S & D Cost #{meter_number}",
            "last_date": "", "last_read": "",
            "new_date": "", "new_read": "",
            "days": "", "consumption": "",
            "rate": "", "due": cost_block["sd_total"]
        },
        {
            "meter_number": "",
            "code": f"Total Water Cost For #{meter_number}",
            "last_date": "", "last_read": "",
            "new_date": "", "new_read": "",
            "days": "", "consumption": "",
            "rate": "", "due": cost_block["total"]
        }
    ])

def build_ws_sd_cost_block(meter_id, month):
    """
    For a given meter, returns detailed WS and SD breakdown rows for Page 2.
    Each row includes: desc, cons, rate, and due.
    Totals are calculated per section and summed as meter total.
    """
    ws_rows = []
    sd_rows = []
    ws_total = 0
    sd_total = 0

    # Pull meter and consumption
    meter = BilMeter.query.get(meter_id)
    cons = BilConsumption.query.filter_by(meter_id=meter_id, month=month).first()
    if not meter or not cons:
        return None

    consumption = cons.consumption
    days = cons.days

    # 🔹 WS Tiered Tariff Rows
    ws_tiers = BilTariff.query.filter_by(utility_type="water").order_by(BilTariff.block_start.asc()).all()
    for tier in ws_tiers:
        block_max = tier.block_end if tier.block_end is not None else float('inf')
        slice_volume = max(0, min(consumption, block_max) - tier.block_start)
        if slice_volume <= 0:
            continue

        charge = round(slice_volume * tier.rate, 2)
        ws_total += charge

        ws_rows.append({
            "desc": f"WS Tier {tier.block_start}–{tier.block_end or '∞'}L / {days} Days",
            "cons": slice_volume,
            "rate": round(tier.rate, 2),
            "due": charge
        })

    # 🔹 SD Tiered Tariff Rows
    sd_tiers = BilTariff.query.filter_by(utility_type="sanitation").order_by(BilTariff.block_start.asc()).all()
    for tier in sd_tiers:
        block_max = tier.block_end if tier.block_end is not None else float('inf')
        slice_volume = max(0, min(consumption, block_max) - tier.block_start)
        if slice_volume <= 0:
            continue

        charge = round(slice_volume * tier.rate, 2)
        sd_total += charge

        sd_rows.append({
            "desc": f"SD Tier {tier.block_start}–{tier.block_end or '∞'}L / {days} Days",
            "cons": slice_volume,
            "rate": round(tier.rate, 2),
            "due": charge
        })

    # 🧾 Combine all subtotals
    total = round(ws_total + sd_total, 2)

    return {
        "meter_number": meter.meter_number,
        "code": getattr(meter, "code", meter.meter_number),
        "ws_rows": ws_rows,
        "sd_rows": sd_rows,
        "ws_total": round(ws_total, 2),
        "sd_total": round(sd_total, 2),
        "total": total
    }

def get_fixed_charge_for_meter(meter_id, month):
    # Placeholder value; link to DB table if needed
    return 45.00

def get_water_loss_levy(meter_id, month):
    return 17.12  # Flat rate per meter

def get_sundry_ws(consumption):
    return round(0.48 * consumption, 2)  # e.g., 48c per KL

def get_sundry_sd(consumption):
    return round(0.32 * consumption, 2)  # e.g., 32c per KL

@billing_bp.route('/metsoa/<int:tenant_id>/<month>', methods=['GET'])
@login_required
def metsoa(tenant_id, month):
    tenant = BilTenant.query.get(tenant_id)
    if not tenant:
        flash("Tenant not found", "danger")
        return redirect(url_for("billing.admin_dashboard"))

    rows = []
    rows += build_electrical_rows(tenant.sectional_unit_id, month)
    rows += build_water_rows(tenant.sectional_unit_id, month)

    return render_template("metsoa.html", tenant=tenant, month=month, rows=rows)

def get_mapped_tenant_by_id(tenant_id):
    user = db.session.query(User).filter_by(id=tenant_id).first()
    if not user:
        return []

    sectional_unit = db.session.query(BilSectionalUnit).filter_by(user_id=user.id).first()
    if not sectional_unit:
        return []

    property_ = db.session.query(BilProperty).filter_by(id=sectional_unit.property_id).first()
    if not property_:
        return []

    return [{
        "username": user.name,
        "sectional_unit_name": sectional_unit.unit_number,
        "property_name": property_.name
    }]

@billing_bp.route('/tenant/<int:tenant_id>')
def tenant_mapping(tenant_id):
    tenant_list = get_mapped_tenant_by_id(tenant_id)
    tenant = tenant_list[0] if tenant_list else None
    return render_template("tenant_mapping.html", tenant=tenant)

def static_page(name):
    return render_template(f"billing/{name}.html")



@billing_bp.route("/healthcheck")
def health_check():
    diagnostics = {}

    # DB Check
    try:
        result = db.session.execute(text("SELECT 1")).scalar()
        diagnostics["database"] = {"status": "✅ healthy", "result": result}
    except Exception as e:
        diagnostics["database"] = {"status": "❌ error", "details": str(e)}

    # Uptime Check
    diagnostics["uptime"] = {"status": "✅ running", "timestamp": time.time()}

    # Optional: Cache / External Service Checks (e.g. Redis, Auth, API)
    # diagnostics["cache"] = cache_check()
    # diagnostics["auth_service"] = auth_ping()

    return jsonify(diagnostics), 200 if diagnostics["database"]["status"] == "✅ healthy" else 500
    
#from helpers.tenants import get_tenant_info

#@billing_bp.route('/debug-tenant/<int:tenant_id>')
#def debug_tenant(tenant_id):
    #info = get_tenant_info(tenant_id)
    #return jsonify(info)