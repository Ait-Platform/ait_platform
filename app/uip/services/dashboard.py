"""Recorded, organisation-scoped operational counts; no SLA inference."""
from app.extensions import db
from app.models.core import CoreInteraction
from app.models.uip import UipWorkOrder
from . import audit, providers


def metrics(org, actor):
    audit.authorize(org, actor, providers.STAFF)
    counts = dict(db.session.query(UipWorkOrder.status, db.func.count(UipWorkOrder.id)).filter(
        UipWorkOrder.organization_id == org).group_by(UipWorkOrder.status).all())
    active = db.session.query(UipWorkOrder.interaction_id).filter(
        UipWorkOrder.organization_id == org, UipWorkOrder.status.notin_(providers.TERMINAL))
    return {
        "Unassigned open issues": CoreInteraction.query.filter(CoreInteraction.organization_id == org,
            db.or_(CoreInteraction.status != "RESOLVED", CoreInteraction.status.is_(None)),
            ~CoreInteraction.id.in_(active)).count(),
        "Created, not dispatched": counts.get("CREATED", 0),
        "Dispatched": counts.get("DISPATCHED", 0),
        "Accepted / in progress": counts.get("ACCEPTED", 0) + counts.get("IN_PROGRESS", 0),
        "Awaiting verification": counts.get("COMPLETED", 0),
        "Verified, awaiting closure": counts.get("VERIFIED", 0),
        "Closed work orders": counts.get("CLOSED", 0),
        "Resolved issues": CoreInteraction.query.filter_by(organization_id=org, status="RESOLVED").count(),
    }
