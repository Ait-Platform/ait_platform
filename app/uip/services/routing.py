"""Explainable provider recommendations; assignment remains a staff action."""
from app.extensions import db
from app.models.uip import UipProvider, UipWorkOrder
from . import audit, providers, operations


def recommend(org, actor, issue_id):
    audit.authorize(org, actor, providers.STAFF)
    issue = operations.issue(org, issue_id)
    workloads = dict(db.session.query(UipWorkOrder.provider_id, db.func.count(UipWorkOrder.id))
        .filter(UipWorkOrder.organization_id == org,
                UipWorkOrder.status.notin_(providers.TERMINAL))
        .group_by(UipWorkOrder.provider_id).all())
    eligible = [p for p in UipProvider.query.filter_by(organization_id=org, is_active=True).all()
                if providers.eligible(org, p, issue.category)]
    return [dict(provider=p, active_workload=workloads.get(p.id, 0))
            for p in sorted(eligible, key=lambda p: (workloads.get(p.id, 0), p.name.casefold(), p.id))]
