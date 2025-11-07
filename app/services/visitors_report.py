# app/services/visitors_report.py
from datetime import datetime, date, time, timedelta
from sqlalchemy import func, desc
from flask import current_app, render_template
from app.extensions import db
from app.models.visit import VisitLog
from app.mailer import send_mail  # you already have send_mail

def send_daily_visitors_report():
    start = datetime.combine(date.today(), time.min)
    end   = start + timedelta(days=1)

    base = db.session.query(VisitLog).filter(VisitLog.ts >= start, VisitLog.ts < end)
    total   = base.count()
    unique  = db.session.query(func.count(func.distinct(VisitLog.ip_hash))).filter(VisitLog.ts >= start, VisitLog.ts < end).scalar()
    top     = db.session.query(VisitLog.path, func.count().label("n")).filter(VisitLog.ts >= start, VisitLog.ts < end)\
                      .group_by(VisitLog.path).order_by(desc("n")).limit(10).all()

    lines = [f"Visitors for {start.date()}",
             f"Total hits: {total}",
             f"Unique IPs: {unique}",
             "",
             "Top paths:"]
    for path, n in top:
        lines.append(f"  {n:>4}  {path}")
    body = "\n".join(lines)

    to_addr = current_app.config.get("FLASK_CONTACT_TO_EMAIL") or "support@mathwithhands.com"
    send_mail(subject=f"[AIT] Visitors {start.date()}", recipients=[to_addr], body=body)
