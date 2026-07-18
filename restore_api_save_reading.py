import re

# Add estimated consumption handling
new_save_reading = """
@billing_bp.route("/api/save_reading", methods=["POST"])
@login_required
def save_reading():
    from app.models import BilMeter, BilConsumption
    data = request.json
    meter_id = data.get("meter_id")
    reading_month = data.get("reading_month")
    
    if not meter_id or not reading_month:
        return {"success": False, "error": "Missing meter_id or reading_month"}, 400
        
    m = BilMeter.query.get(meter_id)
    if not m:
        return {"success": False, "error": "Meter not found"}, 404
        
    if data.get("estimated_consumption") is not None:
        est_cons = float(data.get("estimated_consumption"))
        date_str = data.get("date")
        from datetime import datetime
        new_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        BilConsumption.query.filter_by(meter_id=m.id, month=reading_month).delete()
        
        cons_obj = BilConsumption(
            meter_id=m.id,
            meter_number=m.meter_number,
            last_date=new_date,
            new_date=new_date,
            last_read=0,
            new_read=0,
            days=30,
            consumption=est_cons,
            month=reading_month
        )
        from app.extensions import db
        db.session.add(cons_obj)
        db.session.commit()
        return {"success": True, "message": "Saved"}
        
    # Standard reading save logic
    from datetime import datetime
    new_date = datetime.strptime(data.get("date"), "%Y-%m-%d").date()
    new_reading = float(data.get("reading"))
    
    BilConsumption.query.filter_by(meter_id=m.id, month=reading_month).delete()
    
    prev_reading = float(data.get("prev_reading", 0)) if data.get("prev_reading") else 0
    prev_date_str = data.get("prev_date")
    if prev_date_str:
        prev_date = datetime.strptime(prev_date_str, "%Y-%m-%d").date()
    else:
        from dateutil.relativedelta import relativedelta
        prev_date = new_date - relativedelta(days=30)
        
    days = (new_date - prev_date).days
    if days == 0: days = 30
    
    cons_val = new_reading - prev_reading
    if cons_val < 0: cons_val = 0
    
    cons_obj = BilConsumption(
        meter_id=m.id,
        meter_number=m.meter_number,
        last_date=prev_date,
        new_date=new_date,
        last_read=prev_reading,
        new_read=new_reading,
        days=days,
        consumption=cons_val,
        month=reading_month
    )
    
    from app.extensions import db
    db.session.add(cons_obj)
    db.session.commit()
    return {"success": True, "message": "Saved"}
"""

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes_content = f.read()

if "def save_reading():" not in routes_content:
    routes_content += new_save_reading
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(routes_content)
    print("Added save_reading endpoint.")
else:
    print("save_reading endpoint already exists.")
