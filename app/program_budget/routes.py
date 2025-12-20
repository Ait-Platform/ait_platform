# app/program_budget/routes.py
import csv
from decimal import Decimal
import io
from datetime import date, datetime, timedelta
from flask import Blueprint, redirect, render_template, request, jsonify, abort, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import text
from app.extensions import db, csrf
from app.program_budget.services import budget_summary
from app.models.budget import BudAccount, BudLedger
from types import SimpleNamespace

from . import budget_bp


@budget_bp.post("/import-statement")
@login_required
def import_statement():
    f = request.files.get("file")
    if not f:
        abort(400, "missing file")

    raw_bytes = f.read()

    # quick file-type guard
    if raw_bytes[:4] == b"%PDF":
        abort(400, "Please upload a CSV file (not PDF).")

    # decode + strip UTF-8 BOM if present
    raw = raw_bytes.decode("utf-8-sig", errors="replace")

    rdr = csv.DictReader(io.StringIO(raw))

    fieldnames = [(c or "").strip() for c in (rdr.fieldnames or [])]
    required = {"ext_ref", "txn_date", "amount", "balance"}
    if not required.issubset(set(fieldnames)):
        abort(400, f"CSV must have columns: ext_ref, txn_date, amount, balance (got: {fieldnames})")




    rows = []
    for r in rdr:
        ext_ref = (r.get("ext_ref") or "").strip()
        if not ext_ref:
            continue
        rows.append({
            "ext_ref": ext_ref,
            "txn_date": _parse_date(r.get("txn_date")),
            "description": (r.get("description") or "").strip() or None,
            "amount_cents": _to_cents(r.get("amount")),
            "balance_cents": _to_cents(r.get("balance")),
        })

    # deterministic ordering
    rows.sort(key=lambda x: (x["txn_date"], x["ext_ref"]))

    inserted = 0
    updated = 0

    for r in rows:
        existing = BudLedger.query.filter_by(source="external", ext_ref=r["ext_ref"]).first()
        if existing:
            existing.txn_date = r["txn_date"]
            existing.description = r["description"]
            existing.amount_cents = r["amount_cents"]
            existing.balance_cents = r["balance_cents"]
            updated += 1
        else:
            db.session.add(BudLedger(
                source="external",
                ext_ref=r["ext_ref"],
                txn_date=r["txn_date"],
                description=r["description"],
                amount_cents=r["amount_cents"],
                balance_cents=r["balance_cents"],
            ))
            inserted += 1

    db.session.commit()
    #return jsonify({"ok": True, "inserted": inserted, "updated": updated})
    return redirect(url_for("budget_bp.import_page"))

@csrf.exempt
@budget_bp.get("/summary")
def summary():
    data = budget_summary()
    return jsonify({"ok": True, **data})

@budget_bp.get("/import")
@login_required
def import_page():
    #return render_template("program_budget/import.html")
    return redirect(url_for("budget_bp.ledger"))

@budget_bp.get("/about")
def about():
    return render_template("program_budget/about.html")

@budget_bp.get("/start")
@login_required
def start():
    return redirect(url_for("budget_bp.import_page"))

@budget_bp.get("/dashboard")
@login_required
def dashboard():
    return render_template("program_budget/dashboard.html")

@budget_bp.get("/next")
@login_required
def next_step():
    return redirect(url_for("budget_bp.import_page"))

@budget_bp.route("/ledger", methods=["GET"])
@login_required
def ledger():
    back_url = _safe_next(request.args.get("next")) or url_for("budget_bp.dashboard")
    # --- accounts for dropdown (HIDE hidden) ---
    accounts = db.session.execute(text("""
        SELECT id, name, kind, code, account_no,
               COALESCE(arrears_cents,0) AS arrears_cents,
               COALESCE(balance_cents,0)  AS balance_cents,
               COALESCE(due_cents,0)      AS due_cents
          FROM bud_account
         WHERE user_id = :uid
           AND is_hidden = false
         ORDER BY kind, name
    """), {"uid": current_user.id}).mappings().all()

    # --- lookup selection ---
    selected = None
    account_id = request.args.get("account_id", type=int)

    if account_id and str(account_id).isdigit():
        selected = db.session.execute(text("""
            SELECT id, name, kind, code, account_no,
                   COALESCE(arrears_cents,0) AS arrears_cents,
                   COALESCE(balance_cents,0)  AS balance_cents,
                   COALESCE(due_cents,0)      AS due_cents
              FROM bud_account
             WHERE user_id = :uid
               AND id = :aid
             LIMIT 1
        """), {"uid": current_user.id, "aid": int(account_id)}).mappings().first()

    # --- paid total (to date) for selected ---
    paid_total_cents = 0
    if selected:
        paid_total_cents = db.session.execute(text("""
            SELECT COALESCE(SUM(l.amount_cents),0)
              FROM bud_ledger l
             WHERE l.user_id = :uid
               AND l.account_id = :aid
        """), {"uid": current_user.id, "aid": int(selected["id"])}).scalar() or 0

    # --- ledger rows ---
    rows = db.session.execute(text("""
        SELECT l.id,
            l.txn_date,
            a.name AS account_name,
            l.amount_cents
        FROM bud_ledger l
        JOIN bud_account a ON a.id = l.account_id
        WHERE l.user_id = :uid
        ORDER BY l.txn_date DESC, l.id DESC
        LIMIT 200
    """), {"uid": current_user.id}).mappings().all()

    return render_template(
        "program_budget/ledger.html",
        accounts=accounts,
        selected=selected,
        rows=rows,
        ledger_rows=rows,          # ✅ add this
        paid_total_cents=int(paid_total_cents),
        account_id=account_id,     # keep if you want
        back_url=back_url,
    )

@budget_bp.route("/ledger/<int:ledger_id>/edit", methods=["GET", "POST"])
@login_required
def ledger_edit(ledger_id: int):
    # Fetch entry (must belong to user)
    entry = db.session.execute(text("""
        SELECT l.id, l.account_id, l.txn_date, l.amount_cents
          FROM bud_ledger l
         WHERE l.user_id = :uid AND l.id = :lid
         LIMIT 1
    """), {"uid": int(current_user.id), "lid": int(ledger_id)}).mappings().first()

    if not entry:
        flash("Entry not found.", "warning")
        return redirect(url_for("budget_bp.ledger"))

    # accounts dropdown (hide hidden)
    accounts = db.session.execute(text("""
        SELECT id, name, kind
          FROM bud_account
         WHERE user_id = :uid AND is_hidden = false
         ORDER BY kind, name
    """), {"uid": int(current_user.id)}).mappings().all()

    next_url = (request.values.get("next") or "").strip() or url_for("budget_bp.ledger", account_id=entry["account_id"])

    if request.method == "POST":
        account_id = (request.form.get("account_id") or "").strip()
        txn_date = (request.form.get("txn_date") or "").strip()
        amount = (request.form.get("amount") or "").strip()

        if not (account_id.isdigit() and txn_date and amount):
            flash("Please choose an account, date, and amount.", "warning")
            return redirect(next_url)

        try:
            cents = int(round(float(amount.replace(",", "")) * 100))
        except Exception:
            flash("Invalid amount.", "warning")
            return redirect(next_url)

        try:
            db.session.execute(text("""
                UPDATE bud_ledger
                   SET account_id   = :aid,
                       txn_date     = :d,
                       amount_cents = :c
                 WHERE id = :lid AND user_id = :uid
            """), {
                "aid": int(account_id),
                "d": txn_date,
                "c": int(cents),
                "lid": int(ledger_id),
                "uid": int(current_user.id),
            })
            db.session.commit()
            flash("Entry updated.", "success")
        except Exception:
            db.session.rollback()
            flash("Could not update entry.", "warning")

        return redirect(next_url)

    # prefill amount as 0.00 string
    amount_str = f'{(int(entry["amount_cents"]) / 100):.2f}'

    return render_template(
        "program_budget/ledger_edit.html",
        entry=entry,
        accounts=accounts,
        amount_str=amount_str,
        next_url=next_url,
    )

@budget_bp.route("/ledger/add", methods=["POST"])
@login_required
def ledger_add():
    account_id = (request.form.get("account_id") or "").strip()
    txn_date = (request.form.get("txn_date") or "").strip()
    amount = (request.form.get("amount") or "").strip()

    if not (account_id.isdigit() and txn_date and amount):
        flash("Please choose an account, date, and amount.", "warning")
        return redirect(url_for("budget_bp.ledger"))

    try:
        cents = int(round(float(amount.replace(",", "")) * 100))
    except Exception:
        flash("Invalid amount.", "warning")
        return redirect(url_for("budget_bp.ledger"))

    # ✅ Always show a consistent meaning in the ledger table
    description = "Paid"

    try:
        db.session.execute(text("""
            INSERT INTO bud_ledger (user_id, account_id, txn_date, description, amount_cents)
            VALUES (:uid, :aid, :d, :desc, :c)
        """), {
            "uid": current_user.id,
            "aid": int(account_id),
            "d": txn_date,
            "desc": description,
            "c": int(cents),
        })
        db.session.commit()
        flash("Payment added.", "success")
    except Exception:
        db.session.rollback()
        flash("Could not add entry.", "warning")

    return redirect(url_for(
    "budget_bp.ledger",
    account_id=account_id,
    next=_safe_next(request.args.get("next")) or url_for("budget_bp.dashboard")
    ))

@budget_bp.route("/ledger/account/update", methods=["POST"])
@login_required
def ledger_account_update():
    account_id = (request.form.get("account_id") or "").strip()
    next_url = (request.form.get("next") or "").strip() or url_for("budget_bp.ledger")

    def _to_cents(v: str) -> int:
        v = (v or "").strip()
        if not v:
            return 0
        return int(round(float(v.replace(",", "")) * 100))

    try:
        arrears_cents = _to_cents(request.form.get("arrears") or "0")
        balance_cents = _to_cents(request.form.get("balance") or "0")
        due_cents = _to_cents(request.form.get("due") or "0")
    except Exception:
        flash("Invalid numbers.", "warning")
        return redirect(next_url)

    if not account_id.isdigit():
        flash("Invalid account.", "warning")
        return redirect(next_url)

    try:

        # inside ledger_account_update POST handler, after parsing cents:
        as_at = (request.form.get("as_at") or "").strip() or None

        db.session.execute(text("""
        UPDATE bud_account
            SET arrears_cents = :arrears,
                balance_cents = :balance,
                due_cents     = :due,
                as_at         = :as_at
        WHERE id = :aid AND user_id = :uid
        """), {
        "arrears": arrears_cents,
        "balance": balance_cents,
        "due": due_cents,
        "as_at": as_at,
        "aid": int(account_id),
        "uid": int(current_user.id),
        })


        db.session.commit()
        flash("Account details saved.", "success")
    except Exception:
        db.session.rollback()
        flash("Could not save account details.", "warning")

    return redirect(next_url)

@budget_bp.route("/accounts/new", methods=["GET", "POST"])
@login_required
def account_new():
    next_url = request.args.get("next") or url_for("budget_bp.ledger")
    if not next_url.startswith("/"):
        next_url = url_for("budget_bp.ledger")


    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        kind = (request.form.get("kind") or "").strip().lower()
        account_no = (request.form.get("account_no") or "").strip() or None
        #group_label = (request.form.get("group_label") or "").strip() or None
        group_label = (request.form.get("group_label") or "").strip()

        #is_hidden = (request.form.get("is_hidden") == "1")

        def _slug(s: str) -> str:
            s = (s or "").strip().lower()
            out = []
            prev_dash = False
            for ch in s:
                if ch.isalnum():
                    out.append(ch)
                    prev_dash = False
                else:
                    if not prev_dash:
                        out.append("-")
                        prev_dash = True
            slug = "".join(out).strip("-")
            return slug or "acct"

        if not name or kind not in ("asset", "liability", "expense", "income"):
            flash("Please enter a valid name and type.", "warning")
            return redirect(url_for("budget_bp.account_new", next=next_url))

        code = _slug(name)

        base = code
        i = 2
        while db.session.execute(
            text("SELECT 1 FROM bud_account WHERE user_id=:uid AND code=:code LIMIT 1"),
            {"uid": current_user.id, "code": code},
        ).first():
            code = f"{base}-{i}"
            i += 1

        try:
            db.session.execute(text("""
                INSERT INTO bud_account (user_id, code, name, kind, account_no, group_label)
                VALUES (:uid, :code, :name, :kind, :account_no, :group_label)
            """), {
                "uid": int(current_user.id),
                "code": code,
                "name": name,
                "kind": kind,
                "account_no": account_no,
                "group_label": group_label or "",
            })

            db.session.commit()
        except Exception:
            db.session.rollback()
            flash("Account code or name already exists.", "warning")
            return redirect(url_for("budget_bp.account_new", next=next_url))

        flash("Account created.", "success")
        return redirect(next_url)

    groups = db.session.execute(text("""
        SELECT label
        FROM bud_group_type
        WHERE user_id = :uid
        AND is_active = true
        ORDER BY label
    """), {"uid": int(current_user.id)}).mappings().all()

    return render_template("program_budget/account_new.html", groups=groups, next_url=next_url)

@budget_bp.route("/reports/income-expense", methods=["GET"])
@login_required
def report_income_expense():
    back_url = _safe_next(request.args.get("next")) or url_for("budget_bp.dashboard")    
    period = (request.args.get("period") or "").strip()
    if not period:
        period = db.session.execute(
            text("SELECT to_char(CURRENT_DATE, 'YYYY-MM')")
        ).scalar()

    start_date = f"{period}-01"
    end_date = db.session.execute(
        text("""
            SELECT (
                date_trunc('month', CAST(:d AS date))
                + INTERVAL '1 month - 1 day'
            )::date
        """),
        {"d": start_date},
    ).scalar()

    # -------- INCOME --------
    income_rows = db.session.execute(text("""
        SELECT a.name, SUM(l.amount_cents) AS cents
          FROM bud_ledger l
          JOIN bud_account a ON a.id = l.account_id
         WHERE l.user_id = :uid
           AND l.txn_date BETWEEN :s AND :e
           AND a.kind = 'income'
         GROUP BY a.name
         ORDER BY a.name
    """), {
        "uid": current_user.id,
        "s": start_date,
        "e": end_date,
    }).mappings().all()

    # -------- EXPENSES (expense + liability) --------
    expense_rows = db.session.execute(text("""
        SELECT a.name, SUM(l.amount_cents) AS cents
          FROM bud_ledger l
          JOIN bud_account a ON a.id = l.account_id
         WHERE l.user_id = :uid
           AND l.txn_date BETWEEN :s AND :e
           AND a.kind IN ('expense', 'liability')
         GROUP BY a.name
         ORDER BY a.name
    """), {
        "uid": current_user.id,
        "s": start_date,
        "e": end_date,
    }).mappings().all()

    income_total_cents  = sum(r["cents"] or 0 for r in income_rows)
    expense_total_cents = sum(r["cents"] or 0 for r in expense_rows)
    net_cents = income_total_cents - expense_total_cents

    return render_template(
        "program_budget/report_income_expense.html",
        period=period,
        income_rows=income_rows,
        expense_rows=expense_rows,
        income_total_cents=int(income_total_cents),
        expense_total_cents=int(expense_total_cents),
        net_cents=int(net_cents),
        back_url=back_url,
    )

@budget_bp.route("/help", methods=["GET"])
@login_required
def help_guide():
    return render_template("program_budget/help.html")

@budget_bp.route("/accounts/<int:account_id>", methods=["GET"])
@login_required
def account_detail(account_id: int):
    account = db.session.execute(text("""
        SELECT id, name, kind, account_no,
               COALESCE(arrears_cents,0) AS arrears_cents,
               COALESCE(balance_cents,0) AS balance_cents,
               COALESCE(due_cents,0)     AS due_cents,
               as_at,
               COALESCE(is_hidden,false) AS is_hidden,
               COALESCE(group_label,'')  AS group_label
          FROM bud_account
         WHERE user_id = :uid
           AND id      = :aid
         LIMIT 1
    """), {"uid": int(current_user.id), "aid": int(account_id)}).mappings().first()

    if not account:
        flash("Account not found.", "warning")
        return redirect(url_for("budget_bp.ledger"))

    paid_total_cents = db.session.execute(text("""
        SELECT COALESCE(SUM(amount_cents),0)
          FROM bud_ledger
         WHERE user_id    = :uid
           AND account_id = :aid
    """), {"uid": int(current_user.id), "aid": int(account_id)}).scalar() or 0

    groups = db.session.execute(text("""
        SELECT label
          FROM bud_group_type
         WHERE user_id = :uid
           AND is_active = true
         ORDER BY label
    """), {"uid": int(current_user.id)}).mappings().all()

    return render_template(
        "program_budget/account_detail.html",
        account=account,
        paid_total_cents=int(paid_total_cents),
        groups=groups,
        next_url=url_for("budget_bp.ledger"),
    )

@budget_bp.route("/groups", methods=["GET", "POST"])
@login_required
def group_types():
    if request.method == "POST":
        label = (request.form.get("label") or "").strip()
        if not label:
            flash("Please enter a group name.", "warning")
            return redirect(url_for("budget_bp.group_types"))

        try:
            db.session.execute(text("""
                INSERT INTO bud_group_type (user_id, label, is_active)
                VALUES (:uid, :label, true)
                ON CONFLICT (user_id, label) DO UPDATE SET is_active = true
            """), {"uid": int(current_user.id), "label": label})
            db.session.commit()
            flash("Group type saved.", "success")
        except Exception:
            db.session.rollback()
            flash("Could not save group type.", "warning")

        return redirect(url_for("budget_bp.group_types"))

    groups = db.session.execute(text("""
        SELECT id, label
          FROM bud_group_type
         WHERE user_id = :uid
           AND is_active = true
         ORDER BY label
    """), {"uid": int(current_user.id)}).mappings().all()

    return render_template("program_budget/group_types.html", groups=groups)

@budget_bp.route("/groups/delete", methods=["POST"])
@login_required
def group_type_delete():
    label = (request.form.get("label") or "").strip()
    if not label:
        return redirect(url_for("budget_bp.group_types"))

    try:
        db.session.execute(text("""
            UPDATE bud_group_type
               SET is_active = false
             WHERE user_id = :uid
               AND label = :label
        """), {"uid": int(current_user.id), "label": label})
        db.session.commit()
        flash("Group type removed.", "success")
    except Exception:
        db.session.rollback()
        flash("Could not remove group type.", "warning")

    return redirect(url_for("budget_bp.group_types"))

@budget_bp.route("/groups/add", methods=["POST"])
@login_required
def group_type_add():
    label = (request.form.get("label") or "").strip()
    if not label:
        flash("Please enter a group label.", "warning")
        return redirect(url_for("budget_bp.group_types"))

    try:
        db.session.execute(text("""
            INSERT INTO bud_group_type (user_id, label, is_active)
            VALUES (:uid, :label, true)
            ON CONFLICT (user_id, label) DO UPDATE SET is_active = true
        """), {"uid": int(current_user.id), "label": label})
        db.session.commit()
        flash("Group type added.", "success")
    except Exception:
        db.session.rollback()
        flash("Could not add group type.", "warning")

    return redirect(url_for("budget_bp.group_types"))

@budget_bp.route("/accounts/<int:account_id>/edit", methods=["GET", "POST"])
@login_required
def account_edit(account_id: int):
    account = db.session.execute(text("""
        SELECT id, name, kind, account_no, group_label
          FROM bud_account
         WHERE id = :aid AND user_id = :uid
         LIMIT 1
    """), {"aid": account_id, "uid": current_user.id}).mappings().first()

    if not account:
        flash("Account not found.", "warning")
        return redirect(url_for("budget_bp.ledger"))

    if request.method == "POST":
        kind = (request.form.get("kind") or "").strip().lower()
        group_label = (request.form.get("group_label") or "").strip() or None

        if kind not in ("asset", "liability"):
            flash("Account type must be Asset or Liability.", "warning")
            return redirect(request.url)

        db.session.execute(text("""
            UPDATE bud_account
               SET kind = :kind,
                   group_label = :group
             WHERE id = :aid AND user_id = :uid
        """), {
            "kind": kind,
            "group": group_label,
            "aid": account_id,
            "uid": current_user.id,
        })
        db.session.commit()

        flash("Account updated.", "success")
        return redirect(url_for("budget_bp.ledger", account_id=account_id))

    return render_template(
        "program_budget/account_edit.html",
        account=account,
    )

@budget_bp.route("/accounts/<int:account_id>/meta/update", methods=["POST"])
@login_required
def account_meta_update(account_id: int):
    next_url = (request.form.get("next") or "").strip() or url_for("budget_bp.account_detail", account_id=account_id)

    name = (request.form.get("name") or "").strip()
    kind = (request.form.get("kind") or "").strip().lower()
    account_no = (request.form.get("account_no") or "").strip() or None
    group_label = (request.form.get("group_label") or "").strip()

    if not name or kind not in ("asset", "liability", "expense", "income"):
        flash("Please enter a valid name and type.", "warning")
        return redirect(next_url)

    try:
        db.session.execute(text("""
            UPDATE bud_account
               SET name       = :name,
                   kind       = :kind,
                   account_no = :account_no,
                   group_label= :group_label
             WHERE id = :aid
               AND user_id = :uid
        """), {
            "name": name,
            "kind": kind,
            "account_no": account_no,
            "group_label": group_label,
            "aid": int(account_id),
            "uid": int(current_user.id),
        })
        db.session.commit()
        flash("Account updated.", "success")
    except Exception:
        db.session.rollback()
        flash("Could not update account.", "warning")

    return redirect(next_url)

@budget_bp.route("/reports/by-group", methods=["GET"])
@login_required
def report_by_group():
    # period = YYYY-MM
    period = (request.args.get("period") or "").strip()
    if len(period) != 7 or period[4] != "-":
        today = date.today()
        period = f"{today.year:04d}-{today.month:02d}"

    # month range [start, next_month)
    y = int(period[:4])
    m = int(period[5:7])
    start = f"{y:04d}-{m:02d}-01"
    if m == 12:
        end = f"{y+1:04d}-01-01"
    else:
        end = f"{y:04d}-{m+1:02d}-01"

    # view can be: "group:Retail" or "kind:liability"
    view = (request.args.get("view") or "").strip()
    if not view:
        view = "kind:liability"  # best default: show liabilities at a glance

    groups = db.session.execute(text("""
        SELECT label
          FROM bud_group_type
         WHERE user_id = :uid
           AND is_active = true
         ORDER BY label
    """), {"uid": int(current_user.id)}).mappings().all()

    where_extra = ""
    params = {
        "uid": int(current_user.id),
        "d0": start,
        "d1": end,
    }

    if view.startswith("group:"):
        label = view.split(":", 1)[1].strip()
        where_extra = "AND COALESCE(a.group_label,'') = :glabel"
        params["glabel"] = label
    elif view.startswith("kind:"):
        kind = view.split(":", 1)[1].strip().lower()
        where_extra = "AND a.kind = :kind"
        params["kind"] = kind
    else:
        # fallback (safe)
        where_extra = "AND a.kind = 'liability'"

    rows = db.session.execute(text(f"""
        WITH sel AS (
            SELECT a.id, a.name, a.kind,
                COALESCE(a.group_label,'') AS group_label,
                COALESCE(a.arrears_cents,0) AS arrears_cents,
                COALESCE(a.balance_cents,0)  AS balance_cents,
                COALESCE(a.due_cents,0)      AS due_cents
            FROM bud_account a
            WHERE a.user_id = :uid
            AND COALESCE(a.is_hidden,false) = false
            {where_extra}
        ),
        month_agg AS (
            SELECT l.account_id,
                COALESCE(SUM(l.amount_cents),0) AS paid_month_cents,
                MAX(l.txn_date)                 AS as_at
            FROM bud_ledger l
            WHERE l.user_id = :uid
            AND l.txn_date >= :d0
            AND l.txn_date <  :d1
            GROUP BY l.account_id
        )
        SELECT s.*,
            COALESCE(m.paid_month_cents,0) AS paid_month_cents,
            m.as_at
        FROM sel s
        LEFT JOIN month_agg m ON m.account_id = s.id
        ORDER BY s.kind, s.name
    """), params).mappings().all()


    totals = {
        "arrears_cents": sum(int(r["arrears_cents"] or 0) for r in rows),
        "balance_cents":  sum(int(r["balance_cents"] or 0) for r in rows),
        "due_cents":      sum(int(r["due_cents"] or 0) for r in rows),
        "paid_month_cents": sum(int(r["paid_month_cents"] or 0) for r in rows),
    }

    next_url = (request.args.get("next") or "").strip() or url_for("budget_bp.dashboard")

    return render_template(
        "program_budget/report_by_group.html",
        period=period,
        start=start,
        end=end,
        view=view,
        groups=groups,
        rows=rows,
        totals=totals,
        next_url=next_url,
    )

@budget_bp.before_request
def budget_entitlement_guard():
    if not current_user.is_authenticated:
        return

    now = datetime.utcnow()

    ent = db.session.execute(text("""
        SELECT id, trial_start, trial_end, paid_until
          FROM user_entitlement
         WHERE user_id = :uid
           AND product_slug = 'budgetcash'
         LIMIT 1
    """), {"uid": current_user.id}).mappings().first()

    # First-ever access → start 45-day trial
    if not ent:
        db.session.execute(text("""
            INSERT INTO user_entitlement
              (user_id, product_slug, trial_start, trial_end, last_active)
            VALUES
              (:uid, 'budgetcash', :ts, :te, :now)
        """), {
            "uid": current_user.id,
            "ts": now,
            "te": now + timedelta(days=45),
            "now": now,
        })
        db.session.commit()
        return

    active = (
        (ent["paid_until"] and ent["paid_until"] >= now) or
        (ent["trial_end"] and ent["trial_end"] >= now)
    )

    if not active:
        return redirect(url_for("budget_bp.billing"))  # create simple page later

    # Update last activity
    db.session.execute(text("""
        UPDATE user_entitlement
           SET last_active = :now,
               updated_at = :now
         WHERE id = :id
    """), {"now": now, "id": ent["id"]})
    db.session.commit()

@budget_bp.route("/billing")
@login_required
def billing():
    return render_template("program_budget/billing.html")

def _parse_date(s: str) -> date:
    s = (s or "").strip()
    # best-practice: accept only ISO for v1
    return datetime.strptime(s, "%Y-%m-%d").date()

def _to_cents(s: str) -> int:
    s = (s or "").strip().replace(",", "")
    # expects decimal like 123.45
    sign = -1 if s.startswith("-") else 1
    s = s.lstrip("+-")
    if "." in s:
        whole, frac = s.split(".", 1)
        frac = (frac + "00")[:2]
    else:
        whole, frac = s, "00"
    return sign * (int(whole or "0") * 100 + int(frac))

def ensure_default_budget_accounts(user_id: int):
    existing = db.session.execute(
        text("SELECT 1 FROM bud_account WHERE user_id = :uid LIMIT 1"),
        {"uid": user_id},
    ).first()

    if existing:
        return

    defaults = [
        ("Cash", "asset"),
        ("Bank", "asset"),
        ("Food", "expense"),
        ("Transport", "expense"),
        ("Income", "income"),
    ]

    for name, kind in defaults:
        db.session.execute(
            text("""
                INSERT INTO bud_account (user_id, name, kind)
                VALUES (:uid, :name, :kind)
            """),
            {"uid": user_id, "name": name, "kind": kind},
        )

    db.session.commit()

def _money_to_cents(v: str) -> int:
    v = (v or "").strip().replace(",", "")
    if not v:
        return 0
    return int(round(float(v) * 100))

def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    out = []
    prev_dash = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append("-")
                prev_dash = True
    slug = "".join(out).strip("-")
    return slug or "acct"

def _row_to_obj(row):
    if not row:
        return None
    return SimpleNamespace(**dict(row))

def _safe_next(url: str | None) -> str | None:
    url = (url or "").strip()
    if not url:
        return None
    # allow only local relative paths
    if url.startswith("/") and not url.startswith("//"):
        return url
    return None

