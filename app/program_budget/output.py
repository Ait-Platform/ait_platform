# app/program_budget/output.py
"""
Central BudgetCash Output
Unified reporting facility for Financial Statements, Group Reports, and Snapshot History.
Supports Preview, Browser Print, PDF Download, and Email Dispatch with idempotency.
"""

import calendar
import hashlib
import json
import threading
from datetime import date, datetime
from io import BytesIO

from flask import (
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_login import current_user, login_required
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy import text

from app.extensions import db

# Thread-safe in-flight & sent email deduplication
_SENT_EMAIL_KEYS = set()
_SENT_EMAIL_LOCK = threading.Lock()


def _get_serializer():
    return URLSafeTimedSerializer(
        current_app.secret_key,
        salt="budget-output-v1",
    )


def make_output_token(user_id, report_type, account_id, date_from, date_to, data_digest):
    return _get_serializer().dumps({
        "user_id": user_id,
        "report_type": report_type,
        "account_id": str(account_id or ""),
        "date_from": str(date_from),
        "date_to": str(date_to),
        "digest": data_digest,
        "generated_at": datetime.now().isoformat(),
    })


def verify_output_token(token, user_id, report_type, account_id, date_from, date_to):
    try:
        claim = _get_serializer().loads(token or "", max_age=3600)
    except (BadSignature, SignatureExpired):
        abort(400, description="The report preview has expired or is invalid. Click Preview again.")
    if (
        claim.get("user_id") != user_id
        or claim.get("report_type") != report_type
        or claim.get("account_id") != str(account_id or "")
        or claim.get("date_from") != str(date_from)
        or claim.get("date_to") != str(date_to)
    ):
        abort(400, description="Report parameters changed since preview. Click Preview again before exporting.")
    return claim


def format_currency(cents):
    """Consistent right-aligned currency formatting for South African Rand."""
    if cents is None:
        cents = 0
    val = float(cents) / 100.0
    if val < 0:
        return f"-R {abs(val):,.2f}"
    return f"R {val:,.2f}"


def get_financial_statement_data(user_id, start_date, end_date, account_id=None):
    """
    Computes Income vs Expenses and Balance Sheet Net Worth for the given period and optional account.
    Reuses existing financial statement calculations.
    """
    account_filter = ""
    params = {
        "uid": user_id,
        "s": start_date.strftime("%Y-%m-%d"),
        "e": end_date.strftime("%Y-%m-%d"),
    }
    if account_id:
        account_filter = "AND a.id = :aid"
        params["aid"] = int(account_id)

    # INCOME
    income_rows = db.session.execute(text(f"""
        SELECT a.id, a.name, COALESCE(SUM(l.amount_cents), 0) AS cents
          FROM bud_account a
          LEFT JOIN bud_ledger l ON a.id = l.account_id AND l.user_id = :uid
                                AND l.txn_date BETWEEN :s AND :e
         WHERE a.user_id = :uid
           AND a.kind = 'income'
           AND COALESCE(a.is_hidden, false) = false
           {account_filter}
         GROUP BY a.id, a.name
         ORDER BY a.name
    """), params).mappings().all()

    # EXPENSES & LIABILITIES
    expense_rows = db.session.execute(text(f"""
        SELECT a.id, a.name, COALESCE(SUM(l.amount_cents), 0) AS cents
          FROM bud_account a
          LEFT JOIN bud_ledger l ON a.id = l.account_id AND l.user_id = :uid
                                AND l.txn_date BETWEEN :s AND :e
         WHERE a.user_id = :uid
           AND a.kind IN ('expense', 'liability')
           AND COALESCE(a.is_hidden, false) = false
           {account_filter}
         GROUP BY a.id, a.name
         ORDER BY a.name
    """), params).mappings().all()

    income_total_cents = sum(r["cents"] or 0 for r in income_rows)
    expense_total_cents = sum(r["cents"] or 0 for r in expense_rows)
    net_cents = income_total_cents - expense_total_cents

    active_income = [
        {"id": r["id"], "name": r["name"], "cents": r["cents"], "formatted": format_currency(r["cents"])}
        for r in income_rows if r["cents"] != 0
    ]
    active_expenses = [
        {"id": r["id"], "name": r["name"], "cents": r["cents"], "formatted": format_currency(r["cents"])}
        for r in expense_rows if r["cents"] != 0
    ]

    # NET WORTH / BALANCE SHEET based on latest snapshots
    accounts_q = f"""
        SELECT a.id, a.name, a.kind FROM bud_account a
         WHERE a.user_id = :uid AND a.is_active = 1 AND COALESCE(a.is_hidden, false) = false
         {account_filter}
         ORDER BY a.kind, a.name
    """
    accounts = db.session.execute(text(accounts_q), params).mappings().all()

    snapshots = db.session.execute(text("""
        SELECT account_id, balance_cents, as_at
          FROM bud_snapshot
         WHERE user_id = :uid
           AND as_at <= :e
         ORDER BY as_at DESC, id DESC
    """), {"uid": user_id, "e": end_date.strftime("%Y-%m-%d")}).mappings().all()

    latest_balances = {}
    for s in snapshots:
        if s["account_id"] not in latest_balances:
            latest_balances[s["account_id"]] = s["balance_cents"]

    asset_total_cents = 0
    liability_total_cents = 0
    asset_rows = []
    liability_rows = []

    for a in accounts:
        bal = latest_balances.get(a["id"], 0)
        if bal == 0:
            continue
        row = {"id": a["id"], "name": a["name"], "cents": bal, "formatted": format_currency(bal)}
        if a["kind"] in ("asset", "income"):
            asset_total_cents += bal
            asset_rows.append(row)
        else:
            liability_total_cents += bal
            liability_rows.append(row)

    net_worth_cents = asset_total_cents - liability_total_cents

    return {
        "income_rows": active_income,
        "expense_rows": active_expenses,
        "income_total_cents": income_total_cents,
        "expense_total_cents": expense_total_cents,
        "net_cents": net_cents,
        "formatted_income_total": format_currency(income_total_cents),
        "formatted_expense_total": format_currency(expense_total_cents),
        "formatted_net_cents": format_currency(net_cents),
        "asset_rows": asset_rows,
        "liability_rows": liability_rows,
        "asset_total_cents": asset_total_cents,
        "liability_total_cents": liability_total_cents,
        "net_worth_cents": net_worth_cents,
        "formatted_asset_total": format_currency(asset_total_cents),
        "formatted_liability_total": format_currency(liability_total_cents),
        "formatted_net_worth": format_currency(net_worth_cents),
    }


def get_group_report_data(user_id, start_date, end_date, account_id=None):
    """
    Computes activity and latest balances grouped by account group labels / kinds.
    """
    account_filter = ""
    params = {
        "uid": user_id,
        "d0": start_date.strftime("%Y-%m-%d"),
        "d1": end_date.strftime("%Y-%m-%d"),
    }
    if account_id:
        account_filter = "AND a.id = :aid"
        params["aid"] = int(account_id)

    query = f"""
        WITH sel AS (
            SELECT a.id, a.name, a.kind,
                   COALESCE(a.group_label, '') AS group_label
              FROM bud_account a
             WHERE a.user_id = :uid
               AND COALESCE(a.is_hidden, false) = false
               {account_filter}
        ),
        period_agg AS (
            SELECT l.account_id,
                   COALESCE(SUM(l.amount_cents), 0) AS paid_period_cents
              FROM bud_ledger l
             WHERE l.user_id = :uid
               AND l.txn_date BETWEEN :d0 AND :d1
             GROUP BY l.account_id
        ),
        latest_snaps AS (
            SELECT account_id, arrears_cents, balance_cents, due_cents, as_at
              FROM (
                SELECT account_id, arrears_cents, balance_cents, due_cents, as_at,
                       ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY as_at DESC, id DESC) as rn
                  FROM bud_snapshot
                 WHERE user_id = :uid
                   AND as_at <= :d1
            ) sub
            WHERE rn = 1
        )
        SELECT s.id, s.name, s.kind, s.group_label,
               COALESCE(p.paid_period_cents, 0) AS paid_period_cents,
               COALESCE(sn.arrears_cents, 0) AS arrears_cents,
               COALESCE(sn.balance_cents, 0) AS balance_cents,
               COALESCE(sn.due_cents, 0) AS due_cents,
               sn.as_at AS snapshot_date
          FROM sel s
          LEFT JOIN period_agg p ON p.account_id = s.id
          LEFT JOIN latest_snaps sn ON sn.account_id = s.id
         ORDER BY s.group_label, s.name
    """
    raw_rows = db.session.execute(text(query), params).mappings().all()

    total_paid = 0
    total_arrears = 0
    total_balance = 0
    total_due = 0

    items = []
    for r in raw_rows:
        paid = r["paid_period_cents"] or 0
        arr = r["arrears_cents"] or 0
        bal = r["balance_cents"] or 0
        due = r["due_cents"] or 0

        total_paid += paid
        total_arrears += arr
        total_balance += bal
        total_due += due

        items.append({
            "id": r["id"],
            "name": r["name"],
            "kind": r["kind"],
            "group_label": r["group_label"] or "General",
            "paid_cents": paid,
            "arrears_cents": arr,
            "balance_cents": bal,
            "due_cents": due,
            "formatted_paid": format_currency(paid),
            "formatted_arrears": format_currency(arr),
            "formatted_balance": format_currency(bal),
            "formatted_due": format_currency(due),
            "snapshot_date": r["snapshot_date"].strftime("%d %b %Y") if r["snapshot_date"] else "—",
        })

    return {
        "items": items,
        "rows": items,
        "total_paid_cents": total_paid,
        "total_arrears_cents": total_arrears,
        "total_balance_cents": total_balance,
        "total_due_cents": total_due,
        "formatted_total_paid": format_currency(total_paid),
        "formatted_total_arrears": format_currency(total_arrears),
        "formatted_total_balance": format_currency(total_balance),
        "formatted_total_due": format_currency(total_due),
    }


def get_snapshot_history_data(user_id, start_date, end_date, account_id=None):
    """
    Retrieves chronological snapshot records within the selected date range.
    Does NOT create or charge for new snapshots.
    """
    account_filter = ""
    params = {
        "uid": user_id,
        "s": start_date.strftime("%Y-%m-%d"),
        "e": end_date.strftime("%Y-%m-%d"),
    }
    if account_id:
        account_filter = "AND a.id = :aid"
        params["aid"] = int(account_id)

    query = f"""
        SELECT s.id, s.as_at, s.arrears_cents, s.due_cents, s.balance_cents,
               a.name AS account_name, a.kind AS account_kind,
               COALESCE(a.group_label, '') AS group_label
          FROM bud_snapshot s
          JOIN bud_account a ON a.id = s.account_id AND a.user_id = :uid
         WHERE s.user_id = :uid
           AND s.as_at BETWEEN :s AND :e
           {account_filter}
         ORDER BY s.as_at DESC, a.name
    """
    raw_rows = db.session.execute(text(query), params).mappings().all()

    total_arrears = 0
    total_balance = 0
    total_due = 0

    items = []
    for r in raw_rows:
        arr = r["arrears_cents"] or 0
        bal = r["balance_cents"] or 0
        due = r["due_cents"] or 0

        total_arrears += arr
        total_balance += bal
        total_due += due

        items.append({
            "id": r["id"],
            "as_at": r["as_at"],
            "date_display": r["as_at"].strftime("%d %b %Y") if r["as_at"] else "",
            "account_name": r["account_name"],
            "account_kind": r["account_kind"].capitalize(),
            "group_label": r["group_label"] or "General",
            "arrears_cents": arr,
            "balance_cents": bal,
            "due_cents": due,
            "formatted_arrears": format_currency(arr),
            "formatted_balance": format_currency(bal),
            "formatted_due": format_currency(due),
        })

    return {
        "items": items,
        "rows": items,
        "total_arrears_cents": total_arrears,
        "total_balance_cents": total_balance,
        "total_due_cents": total_due,
        "formatted_total_arrears": format_currency(total_arrears),
        "formatted_total_balance": format_currency(total_balance),
        "formatted_total_due": format_currency(total_due),
    }


def output_view():
    """Central BudgetCash Output handler."""
    user_id = current_user.id

    # 1. Available active accounts for filtering
    accounts = db.session.execute(text("""
        SELECT id, name, kind
          FROM bud_account
         WHERE user_id = :uid AND is_active = 1 AND COALESCE(is_hidden, false) = false
         ORDER BY kind, name
    """), {"uid": user_id}).mappings().all()

    account_map = {a["id"]: a["name"] for a in accounts}

    # 2. Parse request parameters
    args = request.form if request.method == "POST" else request.args

    report_type = (args.get("report_type") or "statement").strip().lower()
    if report_type not in ("statement", "by_group", "snapshots"):
        report_type = "statement"

    account_id_str = (args.get("account_id") or "").strip()
    account_id = None
    if account_id_str and account_id_str.isdigit():
        account_id = int(account_id_str)

    # Date range parsing
    period_param = (args.get("period") or "").strip()
    today = date.today()

    if period_param:
        try:
            start_date = datetime.strptime(f"{period_param}-01", "%Y-%m-%d").date()
            last_day = calendar.monthrange(start_date.year, start_date.month)[1]
            end_date = start_date.replace(day=last_day)
        except ValueError:
            start_date = today.replace(day=1)
            last_day = calendar.monthrange(start_date.year, start_date.month)[1]
            end_date = start_date.replace(day=last_day)
    else:
        date_from_str = (args.get("date_from") or "").strip()
        date_to_str = (args.get("date_to") or "").strip()

        if date_from_str:
            try:
                start_date = datetime.strptime(date_from_str, "%Y-%m-%d").date()
            except ValueError:
                start_date = today.replace(day=1)
        else:
            start_date = today.replace(day=1)

        if date_to_str:
            try:
                end_date = datetime.strptime(date_to_str, "%Y-%m-%d").date()
            except ValueError:
                last_day = calendar.monthrange(start_date.year, start_date.month)[1]
                end_date = start_date.replace(day=last_day)
        else:
            last_day = calendar.monthrange(start_date.year, start_date.month)[1]
            end_date = start_date.replace(day=last_day)

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    date_from = start_date.strftime("%Y-%m-%d")
    date_to = end_date.strftime("%Y-%m-%d")
    period_display = f"{start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}"
    selected_account_name = account_map.get(account_id) if account_id else "All Accounts"

    report_titles = {
        "statement": "BudgetCash Financial Statement",
        "by_group": "BudgetCash Reports by Groups",
        "snapshots": "BudgetCash Snapshot History",
    }
    report_title = report_titles[report_type]

    # 3. Always compute report data if previewed or exporting
    previewed = bool(args.get("preview") or args.get("format") == "pdf" or request.method == "POST")

    report_payload = {}
    preview_token = ""

    if previewed:
        if report_type == "statement":
            report_payload = get_financial_statement_data(user_id, start_date, end_date, account_id)
        elif report_type == "by_group":
            report_payload = get_group_report_data(user_id, start_date, end_date, account_id)
        elif report_type == "snapshots":
            report_payload = get_snapshot_history_data(user_id, start_date, end_date, account_id)

        digest = hashlib.sha256(json.dumps(report_payload, sort_keys=True, default=str).encode()).hexdigest()
        preview_token = make_output_token(user_id, report_type, account_id, date_from, date_to, digest)

    context = {
        "report_type": report_type,
        "account_id": account_id or "",
        "selected_account_name": selected_account_name,
        "date_from": date_from,
        "date_to": date_to,
        "period_display": period_display,
        "report_title": report_title,
        "accounts": accounts,
        "previewed": previewed,
        "preview_token": preview_token,
        "report": report_payload,
        "generated_at": datetime.now().strftime("%d %b %Y, %H:%M"),
        "user_name": current_user.name or current_user.email,
        "email_to": current_user.email,
    }

    # 4. Handle Email Dispatch (POST)
    if request.method == "POST" and args.get("action") == "email":
        raw_token = (args.get("preview_token") or "").strip()
        verify_output_token(raw_token, user_id, report_type, account_id, date_from, date_to)

        email_to = (args.get("email_to") or "").strip()
        if not email_to:
            flash("Recipient email is required.", "error")
            return render_template("program_budget/output.html", **context), 400

        # Validate email
        try:
            from email_validator import EmailNotValidError, validate_email
            email_to = validate_email(email_to, check_deliverability=False).normalized
        except Exception:
            flash("Please enter a valid recipient email address.", "error")
            return render_template("program_budget/output.html", **context), 400

        # Prevent duplicate submissions
        dedup_key = hashlib.sha256(f"{user_id}:{raw_token}:{email_to}".encode()).hexdigest()
        with _SENT_EMAIL_LOCK:
            if dedup_key in _SENT_EMAIL_KEYS:
                flash("This email request has already been submitted and sent. Click Preview again to generate a new report.", "warning")
                return render_template("program_budget/output.html", **context), 409
            _SENT_EMAIL_KEYS.add(dedup_key)

        try:
            from app.utils.mailer import send_pdf_email
            from app.utils.pdf_render import html_to_pdf_bytes

            # Generate PDF for attachment
            pdf_html = render_template("program_budget/output_pdf.html", **context)
            pdf_bytes = html_to_pdf_bytes(pdf_html)
            filename = f"BudgetCash_{report_type}_{date_from}_{date_to}.pdf"

            send_pdf_email(
                to_email=email_to,
                subject=f"{report_title} ({period_display})",
                body_text=f"Please find attached your {report_title} for the period {period_display}.\n\nGenerated from BudgetCash.",
                pdf_bytes=pdf_bytes,
                filename=filename,
            )
            flash(f"Report successfully sent to {email_to}.", "success")
        except Exception as e:
            with _SENT_EMAIL_LOCK:
                _SENT_EMAIL_KEYS.discard(dedup_key)
            current_app.logger.error(f"BudgetCash output email error: {e}")
            flash(f"Failed to send email: {e}", "error")
            return render_template("program_budget/output.html", **context), 500

        return render_template("program_budget/output.html", **context)

    # 5. Handle PDF Download / Save PDF (GET format=pdf)
    if args.get("format") == "pdf":
        raw_token = (args.get("preview_token") or "").strip()
        if raw_token:
            verify_output_token(raw_token, user_id, report_type, account_id, date_from, date_to)

        from app.utils.pdf_render import html_to_pdf_bytes

        pdf_html = render_template("program_budget/output_pdf.html", **context)
        pdf_bytes = html_to_pdf_bytes(pdf_html)
        filename = f"BudgetCash_{report_type}_{date_from}_{date_to}.pdf"

        return send_file(
            BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=bool(args.get("download") == "1"),
            download_name=filename,
        )

    return render_template("program_budget/output.html", **context)
