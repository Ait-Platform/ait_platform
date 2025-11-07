# app/admin/loss/overall_service.py
from sqlalchemy import inspect
from app.extensions import db
from app.models.loss import LcaOverallItem

def _has_col(model, name: str) -> bool:
    insp = inspect(db.engine)
    return any(c["name"] == name for c in insp.get_columns(model.__tablename__))

def band_from_p1(p1_pct: float) -> str:
    try: p = float(p1_pct)
    except Exception: p = 0.0
    if p >= 70: return "high"
    if p >= 40: return "mid"
    return "low"

def get_overall_item_for(p1_pct: float, typ: str = "summary"):
    band = band_from_p1(p1_pct)
    q = (db.session.query(LcaOverallItem)
         .filter(LcaOverallItem.active.is_(True),
                 LcaOverallItem.band == band))
    if _has_col(LcaOverallItem, "type"):
        q = q.filter(LcaOverallItem.type == typ)
    if _has_col(LcaOverallItem, "ordinal"):
        q = q.order_by(LcaOverallItem.ordinal.asc(), LcaOverallItem.id.asc())
    else:
        q = q.order_by(LcaOverallItem.id.asc())
    return q.first()
