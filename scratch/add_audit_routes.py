import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_routes = """
from app.models.core import CoreAuditEvent

@sace_bp.route("/sace/log_event", methods=["POST"])
@login_required
def log_event():
    data = request.get_json()
    action = data.get("action", "UNKNOWN_ACTION")
    details = data.get("details", "")
    
    # Get IP Address (handling proxies)
    ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
    
    event = CoreAuditEvent(
        user_id=current_user.id,
        action=action,
        entity_type="SACE_SIMULATOR",
        details=details,
        ip_address=ip_addr
    )
    db.session.add(event)
    db.session.commit()
    
    return jsonify({"success": True})

@sace_bp.route("/sace/audit_report")
@login_required
def audit_report():
    # Only show events related to SACE and login
    events = CoreAuditEvent.query.order_by(CoreAuditEvent.created_at.desc()).limit(100).all()
    return render_template("program_sace/compliance/audit_report.html", events=events)
"""

if "def log_event" not in text:
    with open('app/program_sace/routes.py', 'a', encoding='utf-8') as f:
        f.write(new_routes)
    print("Added log_event and audit_report routes.")
