"""UIP Finance HTTP boundary; no payment infrastructure integration."""
import csv
import io
import uuid
from datetime import date
from collections import defaultdict
from flask import abort, g, request, render_template, redirect, url_for, flash, Response
from flask_login import current_user, login_required
from sqlalchemy.exc import IntegrityError
from app.extensions import db
from app.uip import uip_bp
from app.uip.services import finance as f, audit, documents
from app.uip.operational_routes import field
from app.models.uip import UipProvider, UipWorkOrder, UipResolution


def context():
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, f.READ)
    return org, actor, f.manager(org, actor)


def render(name, **values):
    org, actor, admin = context()
    return render_template("uip/finance/" + name + ".html", org=g.organization,
        finance_admin=admin, money=lambda v: f"R{v:,.2f}", today=date.today(),
        request_key=str(uuid.uuid4()), notice=f.PUBLIC_NOTICE, **values)


def commit(endpoint, **values):
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        abort(409, description="This financial request conflicts with an existing record. Reload and review before retrying.")
    flash("Financial information recorded in UIP.", "success")
    return redirect(url_for("uip_bp." + endpoint, org_slug=g.organization.slug, **values))


def links_fields(org, actor):
    return [field("provider_id", "Provider (optional)", [("", "No provider")] + [(r.id, r.name) for r in UipProvider.query.filter_by(organization_id=org).all()], required=False),
        field("work_order_id", "Work order (optional)", [("", "No work order")] + [(r.id, r.reference) for r in UipWorkOrder.query.filter_by(organization_id=org).all()], required=False),
        field("governance_decision_id", "Authorising decision (optional)", [("", "No decision")] + [(r.id, r.title) for r in UipResolution.query.filter_by(organization_id=org).all()], required=False),
        field("document_id", "Controlled supporting document (optional)", [("", "No document")] + [(r.id, r.title or "Document #" + str(r.id)) for r in documents.listing(org, actor)], required=False)]


def record_fields(org, actor, transaction=True):
    fields = [field("transaction_date", "Recorded transaction date", kind="date", value=date.today().isoformat()),
        field("amount", "Amount (rand; two decimal places)"), field("category", "Category"),
        field("description", "Management purpose / description", kind="textarea"),
        field("public_description", "Member-safe purpose (required for publication)", kind="textarea", required=False),
        field("public_party", "Member-visible supplier / payer name (optional)", required=False),
        field("visibility", "Transparency approval", [("PRIVATE", "Internal · not published"), ("MEMBERS", "Approved for members")])]
    if transaction:
        fields.insert(0, field("kind", "Transaction type", [("INCOME", "Income"), ("EXPENDITURE", "Expenditure recorded as paid"), ("ADJUSTMENT", "Adjustment to recorded funds")]))
        fields.append(field("party", "Internal payer / payee (optional)", required=False))
        fields.append(field("commitment_id", "Commitment (optional; inherits category, links and publication group)", [("", "No commitment")] + [(r.id, r.reference + " · " + r.category) for r in f.Commitment.query.filter(f.Commitment.organization_id == org, f.Commitment.status.in_(("OPEN", "PARTIALLY_PAID"))).all()], required=False))
        fields.append(field("correction_of_id", "Replacement for reversed transaction (optional)", [("", "Not a correction")] + [(r.reversal_of_id, "Original #" + str(r.reversal_of_id)) for r in f.Transaction.query.filter_by(organization_id=org, status="REVERSAL").all()], required=False))
    return fields + links_fields(org, actor)


@uip_bp.route("/<org_slug>/finance")
@login_required
def finance_overview(org_slug):
    org, actor, admin = context()
    overview = f.overview(org, actor, request.args.get("year"))
    return render("overview", overview=overview,
        major_categories=sorted(overview["budget"], key=lambda b: b["actual"], reverse=True)[:6],
        recent=[f.detail(org, actor, r) for r in overview["transactions"][:10]],
        commitments=[{**f.detail(org, actor, c["row"]), "outstanding": c["outstanding"]} for c in overview["commitments"] if c["outstanding"]])


@uip_bp.route("/<org_slug>/finance/transactions")
@login_required
def finance_transactions(org_slug):
    org, actor, admin = context()
    start, end, label = f.financial_year(request.args.get("year"))
    filters = request.args.to_dict()
    filters.setdefault("from", start.isoformat())
    filters.setdefault("to", end.isoformat())
    rows = f.transactions(org, actor, filters)
    return render("transactions", rows=[f.detail(org, actor, r) for r in rows], filters=filters,
        totals=f.totals(rows), year=start.year, year_label=label)


@uip_bp.route("/<org_slug>/finance/transactions/new", methods=["GET", "POST"])
@login_required
def finance_transaction_new(org_slug):
    org, actor, admin = context()
    audit.authorize(org, actor, f.WRITE)
    if request.method == "POST":
        row = f.create_transaction(org, actor, request.form)
        return commit("finance_transaction", transaction_id=row.id)
    return render("form", title="Record financial transaction", fields=record_fields(org, actor), transaction=True)


@uip_bp.route("/<org_slug>/finance/transactions/<int:transaction_id>", methods=["GET", "POST"])
@login_required
def finance_transaction(org_slug, transaction_id):
    org, actor, admin = context()
    row = f.get(f.Transaction, org, transaction_id)
    detail = f.detail(org, actor, row)
    if request.method == "POST":
        if request.form.get("operation") == "reverse":
            f.reverse_transaction(org, actor, row.id, request.form)
        elif request.form.get("operation") == "visibility":
            f.set_visibility(org, actor, f.Transaction, row.id, request.form)
        else:
            abort(400)
        return commit("finance_transaction", transaction_id=row.id)
    reversal = f.Transaction.query.filter_by(organization_id=org, reversal_of_id=row.id).first()
    return render("detail", record=detail, version=row.version, member_visible=row.member_visible,
        reversible=row.status == "POSTED" and not reversal, commitment=False,
        reversal_reference=reversal.reference if reversal else None)


@uip_bp.route("/<org_slug>/finance/commitments", methods=["GET", "POST"])
@login_required
def finance_commitments(org_slug):
    org, actor, admin = context()
    if request.method == "POST":
        row = f.create_commitment(org, actor, request.form)
        return commit("finance_commitment", commitment_id=row.id)
    overview = f.overview(org, actor, request.args.get("year"))
    return render("commitments", overview=overview,
        rows=[{**f.detail(org, actor, c["row"]), "outstanding": c["outstanding"]} for c in overview["commitments"]],
        fields=record_fields(org, actor, False) if admin else [])


@uip_bp.route("/<org_slug>/finance/commitments/<int:commitment_id>", methods=["GET", "POST"])
@login_required
def finance_commitment(org_slug, commitment_id):
    org, actor, admin = context()
    row = f.get(f.Commitment, org, commitment_id)
    detail = f.detail(org, actor, row)
    if request.method == "POST":
        operation = request.form.get("operation")
        if operation == "visibility":
            f.set_visibility(org, actor, f.Commitment, row.id, request.form)
        elif operation in {"revise", "cancel"}:
            f.change_commitment(org, actor, row.id, request.form, cancel=operation == "cancel")
        else:
            abort(400)
        return commit("finance_commitment", commitment_id=row.id)
    return render("detail", record=detail, version=row.version, member_visible=row.member_visible,
        commitment=True, revisions=f.CommitmentRevision.query.filter_by(organization_id=org, commitment_id=row.id).order_by(f.CommitmentRevision.revision.desc()).all() if admin else [],
        outstanding=f.ZERO if row.status == "CANCELLED" else row.amount-f.paid(org, row.id),
        payments=[f.detail(org, actor, p) for p in f.transactions(org, actor) if p.commitment_id == row.id])


@uip_bp.route("/<org_slug>/finance/budget", methods=["GET", "POST"])
@login_required
def finance_budget(org_slug):
    org, actor, admin = context()
    if request.method == "POST":
        f.revise_budget(org, actor, request.form)
        return commit("finance_budget", year=request.form.get("year"))
    overview = f.overview(org, actor, request.args.get("year"))
    history = []
    if admin:
        line_ids = [b["line"].id for b in overview["budget"] if b["line"]]
        history = f.BudgetRevision.query.filter(f.BudgetRevision.organization_id == org, f.BudgetRevision.budget_line_id.in_(line_ids)).order_by(f.BudgetRevision.recorded_at.desc()).all()
    return render("budget", overview=overview, history=history,
        budget_categories={b["line"].id: b["category"] for b in overview["budget"] if b["line"]},
        decision_fields=links_fields(org, actor)[2:3] if admin else [])


def csv_cell(value):
    if isinstance(value, f.Decimal):
        return str(value)
    value = str(value if value is not None else "")
    return "'" + value if value.lstrip().startswith(("=", "+", "-", "@", "\t", "\r")) else value


@uip_bp.route("/<org_slug>/finance/reports/<report>.csv")
@login_required
def finance_report(org_slug, report):
    org, actor, admin = context()
    overview = f.overview(org, actor, request.args.get("year"))
    # Explicit date-range overrides are offered only for transaction-based reports.
    start, end = overview["start"], overview["end"]
    filters = request.args.to_dict()
    filters.setdefault("from", start.isoformat()); filters.setdefault("to", end.isoformat())
    rows = f.transactions(org, actor, filters)
    if report == "transactions":
        header = ["Reference", "Date", "Type", "Amount (ZAR)", "Category", "Purpose", "Party", "Status", "Work order"]
        records = []
        for row in rows:
            d = f.detail(org, actor, row)
            records.append([d[k] for k in ("reference", "transaction_date", "kind", "amount", "category", "description", "party", "status")] + [d.get("work_order_reference", "")])
    elif report == "income-expenditure":
        t = f.totals(rows)
        header = ["Measure", "Amount (ZAR)"]; records = [[k, v] for k, v in t.items()]
    elif report == "budget":
        header = ["Financial year", "Category", "Budget", "Actual expenditure", "Outstanding commitments", "Remaining"]
        records = [[overview["label"], r["category"], r["approved"], r["actual"], r["committed"], r["remaining"]] for r in overview["budget"]]
    elif report == "commitments":
        header = ["Reference", "Date", "Purpose", "Outstanding (ZAR)"]
        records = [[c["row"].reference, c["row"].transaction_date, f.detail(org, actor, c["row"])["description"], c["outstanding"]] for c in overview["commitments"] if c["outstanding"]]
    elif report in {"category", "provider"}:
        groups = defaultdict(lambda: f.ZERO)
        for row in rows:
            if row.kind == "EXPENDITURE":
                group = row.category if report == "category" else (f.detail(org, actor, row)["party"] or "Not published / no party recorded")
                if report == "provider" and admin and row.provider_id:
                    group = "Provider #" + str(row.provider_id) + ": " + f.get(UipProvider, org, row.provider_id).name
                groups[group] += row.amount
        header = ["Category" if report == "category" else "Supplier / payee", "Recorded expenditure (ZAR)"]
        records = sorted(groups.items())
    else:
        abort(404)
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["UIP recorded financial information", overview["label"], "Management records" if admin else "Approved member-visible records only", "Not bank verified"])
    writer.writerow(["Period", filters["from"], filters["to"]] if report in {"transactions", "income-expenditure", "category", "provider"} else ["Position through", str(overview["as_of"])])
    writer.writerow(header)
    writer.writerows([[csv_cell(c) for c in row] for row in records])
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-Disposition": f'attachment; filename="uip-finance-{report}.csv"', "Cache-Control": "no-store"})
