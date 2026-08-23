import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

tracker_api = '''
@mechanic_bp.route("/mechanic/api/tracker/<reg_number>")
@login_required
def repair_tracker_api(reg_number):
    from app.models.mechanic import MechJobCard, MechVehicle
    from app.models.debtors import Debtor, DebtorLedger
    from flask import jsonify
    import pytz
    
    # Standardize reg number search (remove spaces, uppercase)
    search_reg = reg_number.strip().upper().replace(" ", "")
    
    # Find vehicles matching this reg
    vehicles = MechVehicle.query.join(MechJobCard).filter(
        MechVehicle.license_plate.ilike(f"%{reg_number.strip()}%")
    ).all()
    
    if not vehicles:
        return jsonify({"error": "Vehicle not found"}), 404
        
    # We will aggregate all history for this vehicle's job cards
    timeline = []
    client = vehicles[0].client
    
    for v in vehicles:
        if v.client.user_id != current_user.id:
            continue
            
        for job in v.job_cards:
            # 1. Quote Created (Time In)
            timeline.append({
                "timestamp": job.created_at,
                "date": job.created_at.strftime('%Y-%m-%d'),
                "time": job.created_at.strftime('%H:%M %p'),
                "event": f"Vehicle Received (Quote #{job.job_number})",
                "color": "blue",
                "icon": "fa-car-side"
            })
            
            # 2. Spares Bought/Assigned
            if job.part_lines:
                part_names = ", ".join([f"{p.quantity}x {p.description}" for p in job.part_lines])
                timeline.append({
                    "timestamp": job.created_at, # Approximate since we don't timestamp parts individually yet
                    "date": job.created_at.strftime('%Y-%m-%d'),
                    "time": "Parts Dept",
                    "event": f"Spares Assigned: {part_names}",
                    "color": "emerald",
                    "icon": "fa-cogs"
                })
                
            # 3. Payments Received
            # To get payments for this specific job, we look at the client's ledger around the job date
            # Since ledger is client-level, we'll just grab the client's credits and show them
            # if they occurred after the job card was created.
            
    # Add Payments from the Client Ledger
    if client:
        debtor = Debtor.query.filter_by(name=client.name, user_id=current_user.id).first()
        if debtor:
            for l in debtor.ledgers:
                if l.kind == 'credit':
                    # Add artificial timestamp (midnight) since ledger only has date
                    from datetime import datetime
                    dt = datetime.combine(l.txn_date, datetime.min.time())
                    timeline.append({
                        "timestamp": dt,
                        "date": l.txn_date.strftime('%Y-%m-%d'),
                        "time": "Finance",
                        "event": f"Payment Received: R {l.amount/100.0:.2f} ({l.description})",
                        "color": "indigo",
                        "icon": "fa-money-bill-wave"
                    })
                    
    # Sort timeline by timestamp
    timeline.sort(key=lambda x: x["timestamp"])
    
    # Remove datetime objects before jsonify
    for t in timeline:
        del t["timestamp"]
        
    return jsonify({
        "vehicle": f"{vehicles[0].make or ''} {vehicles[0].model or ''}".strip() or "Vehicle",
        "client": client.name,
        "reg": vehicles[0].license_plate,
        "timeline": timeline
    })
'''

# Insert before @mechanic_bp.route("/mechanic/jobs")
regex = r'(@mechanic_bp\.route\("/mechanic/jobs"\))'
content = re.sub(regex, tracker_api + r'\n\1', content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
