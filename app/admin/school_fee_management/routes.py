from flask import Blueprint, session as flask_session, abort, render_template, current_app, make_response
from sqlalchemy import text as sa_text
from app.extensions import db
from datetime import datetime

sfm_bp = Blueprint(
    "sfm_bp",
    __name__,
    url_prefix="/sfm",
)

@sfm_bp.route('/about', methods=['GET'])
def sfm_about():
    return render_template("admin/school_fee_management/about.html")

def _require_admin():
    """Abort 403 unless current session is admin."""
    role = (flask_session.get("role") or "user").lower()
    if role != "admin":
        abort(403)

def _fetch_all_balances():
    """
    Returns one row per (learner, account).
    Includes fee, total paid, and outstanding.
    """
    sql = sa_text("""
        SELECT
            a.id                AS account_id,
            l.id                AS learner_id,
            l.full_name         AS learner_name,
            l.class_name        AS class_name,
            a.school_year       AS school_year,
            a.annual_fee_cents  AS annual_fee_cents,
            COALESCE(SUM(p.paid_cents), 0) AS total_paid_cents,
            (a.annual_fee_cents - COALESCE(SUM(p.paid_cents), 0)) AS balance_cents
        FROM sfm_account a
        JOIN sfm_learner l ON l.id = a.learner_id
        LEFT JOIN sfm_payment p ON p.account_id = a.id
        GROUP BY
            a.id,
            l.id,
            l.full_name,
            l.class_name,
            a.school_year,
            a.annual_fee_cents
        ORDER BY
            a.school_year DESC,
            l.class_name ASC,
            l.full_name ASC
    """)
    rows = db.session.execute(sql).fetchall()
    return rows

def _fetch_one_learner_detail(learner_id):
    """
    Get learner info, account info, payments list, and computed balance.
    """
    # summary of all accounts for this learner
    summary_sql = sa_text("""
        SELECT
            a.id                AS account_id,
            l.id                AS learner_id,
            l.full_name         AS learner_name,
            l.class_name        AS class_name,
            a.school_year       AS school_year,
            a.annual_fee_cents  AS annual_fee_cents,
            COALESCE(SUM(p.paid_cents), 0) AS total_paid_cents,
            (a.annual_fee_cents - COALESCE(SUM(p.paid_cents), 0)) AS balance_cents
        FROM sfm_account a
        JOIN sfm_learner l ON l.id = a.learner_id
        LEFT JOIN sfm_payment p ON p.account_id = a.id
        WHERE l.id = :lid
        GROUP BY
            a.id,
            l.id,
            l.full_name,
            l.class_name,
            a.school_year,
            a.annual_fee_cents
        ORDER BY a.school_year DESC
    """)
    accounts = db.session.execute(summary_sql, {"lid": learner_id}).fetchall()

    if not accounts:
        return None, None, None  # no such learner or no accounts

    # show payment history for the most recent account (first row after DESC)
    active_account_id = accounts[0].account_id

    payments_sql = sa_text("""
        SELECT
            paid_cents,
            paid_at,
            method
        FROM sfm_payment
        WHERE account_id = :aid
        ORDER BY paid_at DESC, id DESC
    """)
    payments = db.session.execute(payments_sql, {"aid": active_account_id}).fetchall()

    learner_info = {
        "learner_id":   accounts[0].learner_id,
        "learner_name": accounts[0].learner_name,
        "class_name":   accounts[0].class_name,
    }

    return learner_info, accounts, payments

def _fetch_statement(account_id):
    """
    One account's statement: fee, paid, outstanding.
    """
    stmt_sql = sa_text("""
        SELECT
            a.id                AS account_id,
            l.full_name         AS learner_name,
            l.class_name        AS class_name,
            a.school_year       AS school_year,
            a.annual_fee_cents  AS annual_fee_cents,
            COALESCE(SUM(p.paid_cents), 0) AS total_paid_cents,
            (a.annual_fee_cents - COALESCE(SUM(p.paid_cents), 0)) AS balance_cents
        FROM sfm_account a
        JOIN sfm_learner l ON l.id = a.learner_id
        LEFT JOIN sfm_payment p ON p.account_id = a.id
        WHERE a.id = :aid
        GROUP BY
            a.id,
            l.full_name,
            l.class_name,
            a.school_year,
            a.annual_fee_cents
    """)
    row = db.session.execute(stmt_sql, {"aid": account_id}).fetchone()
    return row

@sfm_bp.route("/dashboard")
def sfm_dashboard():
    """
    Admin dashboard for School Fee Management.
    """
    _require_admin()

    rows = _fetch_all_balances()
    # rows[*].account_id, learner_id, learner_name, class_name,
    # school_year, annual_fee_cents, total_paid_cents, balance_cents

    return render_template(
        "admin/school_fee_management/dashboard.html",
        rows=rows,
    )

@sfm_bp.route("/learner/<int:learner_id>")
def sfm_learner_detail(learner_id):
    """
    Admin view of a single learner:
    accounts by year + recent payments.
    """
    _require_admin()

    learner_info, accounts, payments = _fetch_one_learner_detail(learner_id)
    if learner_info is None:
        abort(404)

    return render_template(
        "admin/school_fee_management/learner_detail.html",
        learner=learner_info,
        accounts=accounts,
        payments=payments,
    )

@sfm_bp.route("/statement/<int:account_id>")
def sfm_statement(account_id):
    """
    Generate a plain text statement for sending.
    Later: turn into PDF or email body.
    """
    _require_admin()

    data = _fetch_statement(account_id)
    if not data:
        abort(404)

    def cents_to_rands(cents):
        rands = cents // 100
        cents_part = cents % 100
        return f"R{rands}.{cents_part:02d}"

    body_lines = [
        "Statement of Account",
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"Learner: {data.learner_name}",
        f"Class: {data.class_name}",
        f"School Year: {data.school_year}",
        "",
        f"Annual Fee:      {cents_to_rands(data.annual_fee_cents)}",
        f"Total Paid:      {cents_to_rands(data.total_paid_cents)}",
        f"Outstanding Due: {cents_to_rands(data.balance_cents)}",
        "",
        "Please settle the outstanding balance.",
    ]
    txt = "\n".join(body_lines)

    resp = make_response(txt, 200)
    resp.headers["Content-Type"] = "text/plain; charset=utf-8"
    return resp
