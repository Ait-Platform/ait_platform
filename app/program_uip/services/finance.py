"""Recorded UIP finance. All mutations are scoped, serialized and caller-committed.

March-February years; amounts are rand Decimal. No banking/work-order side effects.
"""
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, DecimalException
import uuid
from flask import abort
from werkzeug.exceptions import Forbidden
from app.extensions import db
from app.models.core import CoreOrganization
from app.models.uip import UipProvider, UipWorkOrder, UipResolution, UipDocument, UipAuditEvent
from app.models.uip_finance import (UipFinanceTransaction as Transaction,
    UipFinanceCommitment as Commitment, UipFinanceBudgetLine as BudgetLine,
    UipFinanceBudgetRevision as BudgetRevision, UipFinanceCommitmentRevision as CommitmentRevision)
from . import audit, documents
from .reception import text, identifier

READ = ("manager", "committee_member", "owner", "resident")
WRITE = ("manager",)
ZERO = Decimal("0.00")
LINKS = {"provider_id": UipProvider, "work_order_id": UipWorkOrder,
         "governance_decision_id": UipResolution, "document_id": UipDocument}
PUBLIC_NOTICE = "Member figures include approved transparency records only; unpublished records are excluded. They may not represent the complete financial position."


def manager(org, actor):
    try:
        audit.authorize(org, actor, WRITE)
        return True
    except Forbidden:
        return False


def money(value, signed=False, zero=False):
    if isinstance(value, (float, bool)):
        abort(400, description="Use a decimal amount in rand with at most two decimal places.")
    try:
        result = Decimal(str(value))
        if not result.is_finite() or result.copy_abs() > Decimal("99999999999999.99") or result != result.quantize(Decimal("0.01")):
            raise ValueError()
        if (not signed and result < 0) or (not zero and result == 0):
            raise ValueError()
    except (DecimalException, ValueError, TypeError):
        abort(400, description="Enter a valid amount in rand, with at most two decimal places.")
    return result.quantize(Decimal("0.01"))


def day(value, future=False):
    try:
        result = date.fromisoformat(str(value))
    except (ValueError, TypeError):
        abort(400, description="Enter a valid date.")
    if not future and result > date.today():
        abort(400, description="Financial record dates cannot be in the future.")
    return result


def financial_year(value=None):
    today = date.today()
    try:
        year = int(value) if value not in (None, "") else today.year - (today.month < 3)
        if not 1900 <= year <= 9998:
            raise ValueError()
    except (ValueError, TypeError):
        abort(400, description="Choose a valid financial year start year.")
    start, end = date(year, 3, 1), date(year + 1, 3, 1) - timedelta(days=1)
    return start, end, f"{year}/{str(year + 1)[-2:]}"


def lock(org, actor):
    audit.authorize(org, actor, WRITE)
    CoreOrganization.query.filter_by(id=org).with_for_update().one()


def get(model, org, row_id):
    return model.query.filter_by(organization_id=org, id=identifier(row_id)).populate_existing().first_or_404()


def expected(row, value):
    if identifier(value) != row.version:
        abort(409, description="This financial record changed. Reload before recording another change.")


def event(org, actor, action, row, **changes):
    # Finance has its own explicit metadata contract; never weakens legacy audit validation.
    audit.authorize(org, actor, WRITE)
    if action not in {"transaction.created", "transaction.reversed", "transaction.corrected",
        "commitment.created", "commitment.changed", "commitment.cancelled", "budget.created",
        "budget.revised", "visibility.changed"} or row.organization_id != org:
        raise ValueError("Invalid Finance audit event")
    db.session.flush()
    db.session.add(UipAuditEvent(organization_id=org, actor_user_id=actor,
        action="finance." + action, entity_type=type(row).__name__, entity_id=row.id,
        metadata_json={"reference": getattr(row, "reference", str(row.id)),
            "changed_fields": sorted(changes),
            **{k: v for k, v in changes.items() if k in {"previous_state", "new_state", "version", "revision"}}}))


def key(value):
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        abort(400, description="Reload the form before submitting.")


def visibility(value):
    if value not in ("PRIVATE", "MEMBERS"):
        abort(400, description="Choose internal or member-approved visibility.")
    return value == "MEMBERS"


def linked(org, actor, values):
    result = {}
    for field, model in LINKS.items():
        obj = get(model, org, values[field]) if values.get(field) else None
        if field == "document_id" and obj and not documents.accessible(org, actor, obj):
            abort(404)
        result[field] = obj.id if obj else None
    if result["work_order_id"]:
        order = get(UipWorkOrder, org, result["work_order_id"])
        if result["provider_id"] and result["provider_id"] != order.provider_id:
            abort(400, description="The selected provider does not match the work order.")
        result["provider_id"] = order.provider_id
    return result


def common(org, actor, values):
    public = visibility(values.get("visibility", "PRIVATE"))
    return dict(transaction_date=day(values.get("transaction_date")),
        category=text(values.get("category"), 100, True).strip(),
        description=text(values.get("description"), 4000, True),
        public_description=text(values.get("public_description"), 2000, public) or "",
        public_party=text(values.get("public_party"), 255) or "", member_visible=public,
        **linked(org, actor, values))


def paid(org, commitment_id, through=None, public=False):
    q = Transaction.query.filter_by(organization_id=org, commitment_id=commitment_id)
    if through:
        q = q.filter(Transaction.transaction_date <= through)
    if public:
        q = q.filter_by(member_visible=True)
    return sum((r.amount for r in q.all()), ZERO)


def sync_commitment(org, actor, row):
    if row.status == "CANCELLED":
        return
    total = paid(org, row.id)
    previous = row.status
    row.status = "PAID" if total == row.amount else "PARTIALLY_PAID" if total else "OPEN"
    row.version += 1
    event(org, actor, "commitment.changed", row, previous_state=previous, new_state=row.status,
          paid=str(total), outstanding=str(row.amount-total), version=row.version)


def commitment_revision(org, actor, row, effective_date, reason="Initial commitment"):
    previous = CommitmentRevision.query.filter_by(organization_id=org, commitment_id=row.id).order_by(CommitmentRevision.revision.desc()).first()
    db.session.add(CommitmentRevision(organization_id=org, commitment_id=row.id,
        revision=previous.revision + 1 if previous else 1, amount=row.amount,
        cancelled=row.status == "CANCELLED", effective_date=effective_date, reason=reason, recorded_by=actor))


def create_commitment(org, actor, values):
    lock(org, actor)
    request_key = key(values.get("request_key"))
    if Commitment.query.filter_by(organization_id=org, request_key=request_key).first():
        abort(409, description="This commitment was already recorded.")
    row = Commitment(organization_id=org, reference="FC-" + uuid.uuid4().hex[:20].upper(),
        amount=money(values.get("amount")), recorded_by=actor, request_key=request_key,
        **common(org, actor, values))
    db.session.add(row)
    event(org, actor, "commitment.created", row, amount=str(row.amount), category=row.category,
        visibility="MEMBERS" if row.member_visible else "PRIVATE")
    commitment_revision(org, actor, row, row.transaction_date)
    return row


def change_commitment(org, actor, row_id, values, cancel=False):
    lock(org, actor)
    row = get(Commitment, org, row_id)
    expected(row, values.get("expected_version"))
    if row.status in {"PAID", "CANCELLED"}:
        abort(409, description="Paid or cancelled commitments cannot be edited.")
    reason = text(values.get("reason"), 2000, True)
    if cancel:
        previous = row.status
        row.status = "CANCELLED"
        event(org, actor, "commitment.cancelled", row, previous_state=previous,
              released=str(row.amount-paid(org, row.id)), reason=reason)
    else:
        amount = money(values.get("amount"))
        if amount < paid(org, row.id):
            abort(409, description="Commitment cannot be reduced below recorded payments.")
        before = str(row.amount)
        row.amount = amount
        event(org, actor, "commitment.changed", row, before_amount=before, after_amount=str(amount), reason=reason)
        sync_commitment(org, actor, row)
    if cancel:
        row.version += 1
    commitment_revision(org, actor, row, date.today(), reason)
    return row


def create_transaction(org, actor, values):
    lock(org, actor)
    request_key = key(values.get("request_key"))
    if Transaction.query.filter_by(organization_id=org, request_key=request_key).first():
        abort(409, description="This transaction was already recorded.")
    kind = values.get("kind")
    if kind not in {"INCOME", "EXPENDITURE", "ADJUSTMENT"}:
        abort(400, description="Choose income, expenditure or adjustment.")
    fields = common(org, actor, values)
    amount = money(values.get("amount"), signed=kind == "ADJUSTMENT")
    commitment = get(Commitment, org, values["commitment_id"]) if values.get("commitment_id") else None
    if commitment:
        if kind != "EXPENDITURE" or commitment.status in {"PAID", "CANCELLED"}:
            abort(409, description="Select an open commitment for recorded expenditure.")
        if fields["transaction_date"] < commitment.transaction_date or amount > commitment.amount - paid(org, commitment.id):
            abort(409, description="Payment date/amount is inconsistent with the commitment.")
        latest_terms = CommitmentRevision.query.filter_by(organization_id=org, commitment_id=commitment.id).order_by(CommitmentRevision.revision.desc()).first()
        if latest_terms and fields["transaction_date"] < latest_terms.effective_date:
            abort(409, description="Payment cannot predate the latest commitment revision.")
        for field in ("provider_id", "work_order_id", "governance_decision_id"):
            if fields[field] and fields[field] != getattr(commitment, field):
                abort(400, description="Payment links must match its commitment.")
            fields[field] = getattr(commitment, field)
        if fields["category"] != commitment.category:
            abort(400, description="Payment category must match its commitment.")
        fields["member_visible"] = commitment.member_visible
        if commitment.member_visible and not fields["public_description"]:
            abort(400, description="Supply a member-safe purpose for this public commitment payment.")
    correction = get(Transaction, org, values["correction_of_id"]) if values.get("correction_of_id") else None
    if correction and (correction.status != "POSTED" or not Transaction.query.filter_by(organization_id=org, reversal_of_id=correction.id).first()):
        abort(409, description="Reverse the original transaction before recording its replacement.")
    if correction and Transaction.query.filter_by(organization_id=org, correction_of_id=correction.id).first():
        abort(409, description="A replacement already exists for this transaction.")
    row = Transaction(organization_id=org, reference="FT-" + uuid.uuid4().hex[:20].upper(),
        kind=kind, amount=amount, party=text(values.get("party"), 255) or "", recorded_by=actor,
        request_key=request_key, commitment_id=commitment.id if commitment else None,
        correction_of_id=correction.id if correction else None, **fields)
    db.session.add(row)
    event(org, actor, "transaction.corrected" if correction else "transaction.created", row,
        amount=str(amount), kind=kind, category=row.category, correction_of=correction.id if correction else None)
    if commitment:
        sync_commitment(org, actor, commitment)
    return row


def reverse_transaction(org, actor, row_id, values):
    lock(org, actor)
    row = get(Transaction, org, row_id)
    expected(row, values.get("expected_version"))
    if row.status != "POSTED" or Transaction.query.filter_by(organization_id=org, reversal_of_id=row.id).first():
        abort(409, description="This transaction cannot be reversed again.")
    reversal_date = day(values.get("transaction_date"))
    if reversal_date < row.transaction_date:
        abort(400, description="A reversal cannot predate the original transaction.")
    reason = text(values.get("reason"), 2000, True)
    fields = {name: getattr(row, name) for name in (*LINKS, "kind", "category", "party",
        "public_description", "public_party", "member_visible", "commitment_id")}
    request_key = key(values.get("request_key"))
    if Transaction.query.filter_by(organization_id=org, request_key=request_key).first():
        abort(409, description="This request was already recorded.")
    reverse = Transaction(organization_id=org, reference="FR-" + uuid.uuid4().hex[:20].upper(),
        amount=-row.amount, transaction_date=reversal_date, description="Reversal: " + row.reference,
        correction_reason=reason, reversal_of_id=row.id, status="REVERSAL", recorded_by=actor,
        request_key=request_key, **fields)
    db.session.add(reverse)
    event(org, actor, "transaction.reversed", reverse, original_reference=row.reference,
        original_id=row.id, amount=str(reverse.amount), reason=reason)
    if row.commitment_id:
        sync_commitment(org, actor, get(Commitment, org, row.commitment_id))
    return reverse


def set_visibility(org, actor, model, row_id, values):
    lock(org, actor)
    row = get(model, org, row_id)
    expected(row, values.get("expected_version"))
    public = visibility(values.get("visibility"))
    # A commitment and every payment/reversal are one publication group.
    root = row if model is Commitment else get(Commitment, org, row.commitment_id) if row.commitment_id else None
    if root:
        rows = [root] + Transaction.query.filter_by(organization_id=org, commitment_id=root.id).all()
    else:
        original_id = row.reversal_of_id or row.id
        rows = Transaction.query.filter(Transaction.organization_id == org,
            db.or_(Transaction.id == original_id, Transaction.reversal_of_id == original_id)).all()
    if public and any(not r.public_description for r in rows):
        abort(409, description="Every linked record requires a member-safe purpose before publication. Reverse/correct records with missing or unsafe public text.")
    for item in rows:
        if item.member_visible != public:
            item.member_visible = public
            item.version += 1
            event(org, actor, "visibility.changed", item, member_visible=public, version=item.version)
    return row


def revise_budget(org, actor, values):
    lock(org, actor)
    start, end, label = financial_year(values.get("year"))
    category = text(values.get("category"), 100, True).strip()
    row = BudgetLine.query.filter_by(organization_id=org, year_start=start, category=category).first()
    previous = None
    if row:
        previous = BudgetRevision.query.filter_by(organization_id=org, budget_line_id=row.id).order_by(BudgetRevision.revision.desc()).first()
        if identifier(values.get("expected_revision")) != previous.revision:
            abort(409, description="Budget was revised. Reload the current revision.")
    else:
        if str(values.get("expected_revision", "0")) != "0":
            abort(409)
        row = BudgetLine(organization_id=org, year_start=start, category=category)
        db.session.add(row); db.session.flush()
    decision = get(UipResolution, org, values["governance_decision_id"]) if values.get("governance_decision_id") else None
    revised_date = day(values.get("revised_date"))
    if previous and revised_date < previous.revised_date:
        abort(400, description="A revision cannot predate the current budget revision.")
    revision = BudgetRevision(organization_id=org, budget_line_id=row.id,
        revision=previous.revision + 1 if previous else 1, approved_amount=money(values.get("amount"), zero=True),
        description=text(values.get("description"), 2000, True), revised_date=revised_date,
        governance_decision_id=decision.id if decision else None, recorded_by=actor)
    db.session.add(revision)
    event(org, actor, "budget.revised" if previous else "budget.created", row, year=label,
        before_amount=str(previous.approved_amount) if previous else None,
        approved_amount=str(revision.approved_amount), revision=revision.revision)
    return row


def transactions(org, actor, filters=None):
    audit.authorize(org, actor, READ)
    q = Transaction.query.filter_by(organization_id=org)
    if not manager(org, actor):
        q = q.filter_by(member_visible=True)
    f = filters or {}
    for name, comparison in (("from", Transaction.transaction_date.__ge__), ("to", Transaction.transaction_date.__le__)):
        if f.get(name):
            q = q.filter(comparison(day(f[name], future=True)))
    if f.get("from") and f.get("to") and day(f["from"], future=True) > day(f["to"], future=True):
        abort(400, description="Date range is reversed.")
    if f.get("kind"):
        if f["kind"] not in {"INCOME", "EXPENDITURE", "ADJUSTMENT"}:
            abort(400)
        q = q.filter_by(kind=f["kind"])
    if f.get("category"):
        q = q.filter(Transaction.category.ilike("%" + f["category"].replace("%", r"\%").replace("_", r"\_") + "%"))
    if f.get("party"):
        column = Transaction.party if manager(org, actor) else Transaction.public_party
        matches = [column.ilike("%" + f["party"] + "%"), Transaction.public_party.ilike("%" + f["party"] + "%")]
        if manager(org, actor):
            matches.append(Transaction.provider_id.in_(db.session.query(UipProvider.id).filter(UipProvider.organization_id == org, UipProvider.name.ilike("%" + f["party"] + "%"))))
        q = q.filter(db.or_(*matches))
    if f.get("provider_id"):
        q = q.filter_by(provider_id=identifier(f["provider_id"]))
    return q.order_by(Transaction.transaction_date.desc(), Transaction.id.desc()).all()


def totals(rows):
    result = dict(income=ZERO, expenditure=ZERO, adjustment=ZERO)
    for row in rows:
        result[{"INCOME": "income", "EXPENDITURE": "expenditure", "ADJUSTMENT": "adjustment"}[row.kind]] += row.amount
    result["cash"] = result["income"] - result["expenditure"] + result["adjustment"]
    return result


def overview(org, actor, year=None):
    audit.authorize(org, actor, READ)
    start, end, label = financial_year(year)
    as_of = min(end, date.today())
    rows = transactions(org, actor)
    period = [r for r in rows if start <= r.transaction_date <= as_of]
    opening = totals([r for r in rows if r.transaction_date < start])["cash"]
    movement = totals(period)
    cash = opening + movement["cash"]
    cq = Commitment.query.filter(Commitment.organization_id == org, Commitment.transaction_date <= as_of)
    if not manager(org, actor):
        cq = cq.filter_by(member_visible=True)
    commitments = []
    for c in cq.order_by(Commitment.id.desc()).all():
        snapshot = CommitmentRevision.query.filter(CommitmentRevision.organization_id == org, CommitmentRevision.commitment_id == c.id, CommitmentRevision.effective_date <= as_of).order_by(CommitmentRevision.revision.desc()).first()
        outstanding = ZERO if not snapshot or snapshot.cancelled else snapshot.amount - paid(org, c.id, as_of, not manager(org, actor))
        commitments.append(dict(row=c, outstanding=outstanding))
    outstanding = sum((c["outstanding"] for c in commitments), ZERO)
    lines = BudgetLine.query.filter_by(organization_id=org, year_start=start).all()
    budget = []
    categories = set(r.category for r in period if r.kind == "EXPENDITURE") | {c["row"].category for c in commitments if c["outstanding"]} | {l.category for l in lines}
    for category in sorted(categories):
        line = next((l for l in lines if l.category == category), None)
        revision = BudgetRevision.query.filter(BudgetRevision.organization_id == org, BudgetRevision.budget_line_id == line.id, BudgetRevision.revised_date <= as_of).order_by(BudgetRevision.revision.desc()).first() if line else None
        approved = revision.approved_amount if revision else ZERO
        actual = sum((r.amount for r in period if r.kind == "EXPENDITURE" and r.category == category), ZERO)
        committed = sum((c["outstanding"] for c in commitments if c["row"].category == category), ZERO)
        budget.append(dict(line=line, revision=revision, category=category, approved=approved,
            actual=actual, committed=committed, remaining=approved-actual-committed))
    annual = sum((r["approved"] for r in budget), ZERO)
    return dict(start=start, end=end, label=label, as_of=as_of, opening=opening,
        **{**movement, "cash": cash}, outstanding=outstanding, available=cash-outstanding, annual=annual,
        remaining=annual-movement["expenditure"]-outstanding, budget=budget, transactions=period,
        commitments=commitments, public=not manager(org, actor))


def detail(org, actor, row):
    audit.authorize(org, actor, READ)
    admin = manager(org, actor)
    if row.organization_id != org or (not admin and not row.member_visible):
        abort(404)
    result = dict(id=row.id, reference=row.reference, transaction_date=row.transaction_date,
        amount=row.amount, category=row.category, description=row.description if admin else row.public_description,
        party=(getattr(row, "party", "") or row.public_party) if admin else row.public_party,
        kind=getattr(row, "kind", "Commitment"), status=row.status, links=[],
        reversal_of_id=getattr(row, "reversal_of_id", None), correction_of_id=getattr(row, "correction_of_id", None))
    if admin:
        result["management_reason"] = getattr(row, "correction_reason", None)
    # Only curated financial purpose/party is published, never raw provider/decision descriptions.
    if row.work_order_id:
        order = get(UipWorkOrder, org, row.work_order_id)
        result["work_order_reference"] = order.reference
        if admin:
            result["links"].append(("Work order " + order.reference, "work_order_view", {"order_id": order.id}))
    if admin and row.provider_id:
        result["links"].append(("Provider record", "provider_view", {"provider_id": row.provider_id}))
    if row.governance_decision_id:
        result["decision_reference"] = "Decision #" + str(row.governance_decision_id)
        if admin:
            result["links"].append(("Governance decision", "decisions_page", {}))
    if row.document_id:
        document = get(UipDocument, org, row.document_id)
        if document.current_version and documents.accessible(org, actor, document) and (admin or document.access_classification == "MEMBERS"):
            result["links"].append(("Supporting document", "document_download", {"document_id": document.id, "version": document.current_version}))
    return result
