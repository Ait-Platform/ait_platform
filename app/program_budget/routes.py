# app/program_budget/routes.py
import csv
from decimal import Decimal
import io
import secrets
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta
from flask import Blueprint, redirect, render_template, request, jsonify, abort, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import text
from app.extensions import db, csrf
from app.program_budget.services import budget_summary
from app.models.budget import BudAccount, BudLedger
from types import SimpleNamespace

# In-memory stores for submission idempotency
import threading
_SUBMISSION_LOCK = threading.Lock()
_IN_FLIGHT_SUBMISSIONS = set()
_PROCESSED_SUBMISSIONS = OrderedDict()
_PROCESSED_RECENT_POSTS = OrderedDict()

def _prune_idempotency_cache():
    while _PROCESSED_SUBMISSIONS and len(_PROCESSED_SUBMISSIONS) > 1000:
        _PROCESSED_SUBMISSIONS.popitem(last=False)
    while _PROCESSED_RECENT_POSTS and len(_PROCESSED_RECENT_POSTS) > 1000:
        _PROCESSED_RECENT_POSTS.popitem(last=False)

def _is_submission_processed(user_id: int, submission_id: str) -> bool:
    _prune_idempotency_cache()
    return (user_id, submission_id) in _PROCESSED_SUBMISSIONS

def _mark_submission_processed(user_id: int, submission_id: str):
    _prune_idempotency_cache()
    _PROCESSED_SUBMISSIONS[(user_id, submission_id)] = time.time()

def _is_recent_duplicate(user_id: int, account_id: int, txn_date: str, cents: int, window_seconds: float = 10.0) -> bool:
    _prune_idempotency_cache()
    key = (user_id, account_id, str(txn_date), cents)
    now = time.time()
    if key in _PROCESSED_RECENT_POSTS:
        if now - _PROCESSED_RECENT_POSTS[key] < window_seconds:
            return True
    try:
        row = db.session.execute(text("""
            SELECT id FROM bud_ledger
             WHERE user_id = :uid AND account_id = :aid AND txn_date = :d AND amount_cents = :c
               AND created_at >= (NOW() - INTERVAL '10 seconds')
             ORDER BY id DESC LIMIT 1
        """), {"uid": user_id, "aid": account_id, "d": txn_date, "c": cents}).scalar()
        if row is not None:
            _PROCESSED_RECENT_POSTS[key] = now
            return True
    except Exception:
        pass
    return False

def _record_recent_submission(user_id: int, account_id: int, txn_date: str, cents: int):
    _prune_idempotency_cache()
    key = (user_id, account_id, str(txn_date), cents)
    _PROCESSED_RECENT_POSTS[key] = time.time()

from . import budget_bp

# Pending CSV imports awaiting user confirmation before token deduction
_PENDING_IMPORTS = {}
_PENDING_IMPORTS_LOCK = threading.Lock()

def _prune_pending_imports():
    now = time.time()
    with _PENDING_IMPORTS_LOCK:
        expired = [k for k, v in list(_PENDING_IMPORTS.items()) if now - v.get("created_at", 0) > 1800]
        for k in expired:
            _PENDING_IMPORTS.pop(k, None)


@budget_bp.post("/import-statement")
@login_required
def import_statement():
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    _prune_pending_imports()

    action = (request.form.get("action") or "").strip().lower()

    # User confirmed the staged CSV import
    if action == "confirm":
        import_id = (request.form.get("import_id") or "").strip()
        with _PENDING_IMPORTS_LOCK:
            pending = _PENDING_IMPORTS.pop((current_user.id, import_id), None)

        if not pending:
            flash("Import session expired or already processed.", "warning")
            return redirect(url_for("budget_bp.import_page")), 302

        target_aid = pending["account_id"]
        rows = pending["rows"]
        tokens_required = pending["tokens_required"]

        with db.session.no_autoflush:
            wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).populate_existing().with_for_update().first()
            if not wallet or wallet.balance < tokens_required:
                db.session.rollback()
                flash("Insufficient tokens. Purchase more tokens to continue.", "error")
                return redirect(url_for("payment_bp.wallet_topup", subject_slug="budget")), 302

            inserted = 0
            try:
                for r in rows:
                    db.session.execute(text("""
                        INSERT INTO bud_ledger (user_id, account_id, txn_date, description, amount_cents, source)
                        VALUES (:uid, :aid, :d, :desc, :c, 'external')
                    """), {
                        "uid": current_user.id,
                        "aid": target_aid,
                        "d": r["txn_date"],
                        "desc": r["description"],
                        "c": r["amount_cents"],
                    })
                    inserted += 1

                    if r.get("balance_cents") is not None:
                        db.session.execute(text("""
                            UPDATE bud_account
                               SET balance_cents = :bal, as_at = :d
                             WHERE id = :aid AND user_id = :uid
                        """), {"bal": r["balance_cents"], "d": r["txn_date"], "aid": target_aid, "uid": current_user.id})

                        db.session.execute(text("""
                            DELETE FROM bud_snapshot
                             WHERE user_id = :uid AND account_id = :aid AND as_at = :d
                        """), {"uid": current_user.id, "aid": target_aid, "d": r["txn_date"]})
                        db.session.execute(text("""
                            INSERT INTO bud_snapshot (user_id, account_id, as_at, balance_cents, arrears_cents, due_cents)
                            VALUES (:uid, :aid, :d, :bal, 0, 0)
                        """), {"uid": current_user.id, "aid": target_aid, "d": r["txn_date"], "bal": r["balance_cents"]})

                if inserted > 0:
                    wallet.balance -= inserted
                    tx = AitTokenTransaction(
                        wallet_id=wallet.id,
                        amount=-inserted,
                        description=f"CSV Statement Import ({inserted} transaction{'s' if inserted != 1 else ''})"
                    )
                    db.session.add(tx)

                db.session.commit()
                if inserted > 0:
                    flash(f"Successfully imported {inserted} transaction(s). {inserted} token{'s' if inserted != 1 else ''} deducted.", "success")
                else:
                    flash("No new transactions imported.", "info")
                return redirect(url_for("budget_bp.ledger", account_id=target_aid))
            except Exception:
                db.session.rollback()
                flash("Could not complete CSV import. No tokens were deducted.", "warning")
                return redirect(url_for("budget_bp.import_page")), 302

    f = request.files.get("file")
    if not f or not f.filename:
        flash("Please upload a CSV file.", "warning")
        return redirect(url_for("budget_bp.import_page")), 302

    raw_bytes = f.read()

    # quick file-type guard
    if raw_bytes[:4] == b"%PDF":
        flash("Please upload a CSV file (not PDF).", "warning")
        return redirect(url_for("budget_bp.import_page")), 302

    # decode + strip UTF-8 BOM if present
    raw = raw_bytes.decode("utf-8-sig", errors="replace")

    rdr = csv.DictReader(io.StringIO(raw))

    fieldnames = [(c or "").strip().lower() for c in (rdr.fieldnames or [])]
    required = {"ext_ref", "txn_date", "amount"}
    alt_required = {"date", "amount"}
    if not required.issubset(set(fieldnames)) and not alt_required.issubset(set(fieldnames)):
        flash(f"CSV must contain headings: ext_ref, txn_date, amount, balance (got: {', '.join(fieldnames)})", "danger")
        return redirect(url_for("budget_bp.import_page")), 302

    # Resolve target account
    target_account_id = (request.form.get("account_id") or "").strip()
    target_acc_name = "Default Account"
    if target_account_id and target_account_id.isdigit():
        _require_owned_account(int(target_account_id))
        target_aid = int(target_account_id)
        acc_rec = db.session.execute(text("SELECT name FROM bud_account WHERE id=:aid AND user_id=:uid"), {"aid": target_aid, "uid": current_user.id}).first()
        if acc_rec:
            target_acc_name = acc_rec[0]
    else:
        # Find existing default asset / bank account for this user
        acc_row = db.session.execute(text("""
            SELECT id, name FROM bud_account 
             WHERE user_id = :uid AND (lower(name) = 'bank' OR kind = 'asset')
             ORDER BY id ASC LIMIT 1
        """), {"uid": current_user.id}).first()
        if acc_row:
            target_aid = acc_row[0]
            target_acc_name = acc_row[1]
        else:
            acc_row = db.session.execute(text("""
                SELECT id, name FROM bud_account WHERE user_id = :uid ORDER BY id ASC LIMIT 1
            """), {"uid": current_user.id}).first()
            if acc_row:
                target_aid = acc_row[0]
                target_acc_name = acc_row[1]
            else:
                new_acc = db.session.execute(text("""
                    INSERT INTO bud_account (user_id, name, kind, code)
                    VALUES (:uid, 'Bank', 'asset', 'bank')
                    RETURNING id
                """), {"uid": current_user.id}).fetchone()
                target_aid = new_acc[0]
                target_acc_name = "Bank"
                db.session.commit()

    rows = []
    duplicate_count = 0
    invalid_count = 0

    for r in rdr:
        r_norm = {k.strip().lower(): (v or "").strip() for k, v in r.items() if k}
        ext_ref = r_norm.get("ext_ref") or r_norm.get("ref") or r_norm.get("id") or ""
        date_str = r_norm.get("txn_date") or r_norm.get("date") or ""
        amt_str = r_norm.get("amount") or ""
        bal_str = r_norm.get("balance") or ""
        desc_str = r_norm.get("description") or r_norm.get("desc") or ""

        if not (date_str and amt_str):
            invalid_count += 1
            continue

        try:
            parsed_date = _parse_date(date_str)
            cents = _to_cents(amt_str)
        except Exception:
            invalid_count += 1
            continue

        bal_cents = None
        if bal_str:
            try:
                bal_cents = _to_cents(bal_str)
            except Exception:
                bal_cents = None

        formatted_desc = f"[{ext_ref}] {desc_str}".strip() if ext_ref else (desc_str or "External Import")

        # Deduplication check against existing records
        existing = db.session.execute(text("""
            SELECT id FROM bud_ledger
             WHERE user_id = :uid AND account_id = :aid AND txn_date = :d AND amount_cents = :c
               AND source = 'external' AND (description = :desc OR description LIKE :ref_prefix)
             LIMIT 1
        """), {
            "uid": current_user.id,
            "aid": target_aid,
            "d": parsed_date,
            "c": cents,
            "desc": formatted_desc,
            "ref_prefix": f"[{ext_ref}]%" if ext_ref else f"%{formatted_desc}%",
        }).first()

        if existing:
            duplicate_count += 1
            continue

        rows.append({
            "ext_ref": ext_ref,
            "txn_date": parsed_date,
            "description": formatted_desc,
            "amount_cents": cents,
            "balance_cents": bal_cents,
        })

    # deterministic ordering
    rows.sort(key=lambda x: (x["txn_date"], x["ext_ref"]))
    valid_count = len(rows)
    excluded_count = duplicate_count + invalid_count
    tokens_required = valid_count

    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    wallet_balance = wallet.balance if wallet else 0

    # Direct confirmation flag (for headless tests or direct form submissions)
    if request.form.get("confirm") == "1":
        if tokens_required > 0:
            wallet_lock = AitTokenWallet.query.filter_by(user_id=current_user.id).populate_existing().with_for_update().first()
            if not wallet_lock or wallet_lock.balance < tokens_required:
                db.session.rollback()
                flash("Insufficient tokens. Purchase more tokens to continue.", "error")
                return redirect(url_for("payment_bp.wallet_topup", subject_slug="budget")), 302

            inserted = 0
            for r in rows:
                db.session.execute(text("""
                    INSERT INTO bud_ledger (user_id, account_id, txn_date, description, amount_cents, source)
                    VALUES (:uid, :aid, :d, :desc, :c, 'external')
                """), {
                    "uid": current_user.id,
                    "aid": target_aid,
                    "d": r["txn_date"],
                    "desc": r["description"],
                    "c": r["amount_cents"],
                })
                inserted += 1

                if r.get("balance_cents") is not None:
                    db.session.execute(text("""
                        UPDATE bud_account
                           SET balance_cents = :bal, as_at = :d
                         WHERE id = :aid AND user_id = :uid
                    """), {"bal": r["balance_cents"], "d": r["txn_date"], "aid": target_aid, "uid": current_user.id})

                    db.session.execute(text("""
                        DELETE FROM bud_snapshot
                         WHERE user_id = :uid AND account_id = :aid AND as_at = :d
                    """), {"uid": current_user.id, "aid": target_aid, "d": r["txn_date"]})
                    db.session.execute(text("""
                        INSERT INTO bud_snapshot (user_id, account_id, as_at, balance_cents, arrears_cents, due_cents)
                        VALUES (:uid, :aid, :d, :bal, 0, 0)
                    """), {"uid": current_user.id, "aid": target_aid, "d": r["txn_date"], "bal": r["balance_cents"]})

            wallet_lock.balance -= inserted
            tx = AitTokenTransaction(
                wallet_id=wallet_lock.id,
                amount=-inserted,
                description=f"CSV Statement Import ({inserted} transaction{'s' if inserted != 1 else ''})"
            )
            db.session.add(tx)
            db.session.commit()
            flash(f"Successfully imported {inserted} transaction(s). {inserted} token{'s' if inserted != 1 else ''} deducted.", "success")
        else:
            flash(f"No new transactions imported. {duplicate_count} duplicate(s) skipped.", "info")
        return redirect(url_for("budget_bp.ledger", account_id=target_aid)), 302

    # Standard UI flow: Display confirmation preview screen
    import_id = secrets.token_hex(16)
    with _PENDING_IMPORTS_LOCK:
        _PENDING_IMPORTS[(current_user.id, import_id)] = {
            "account_id": target_aid,
            "account_name": target_acc_name,
            "rows": rows,
            "valid_count": valid_count,
            "duplicate_count": duplicate_count,
            "invalid_count": invalid_count,
            "tokens_required": tokens_required,
            "created_at": time.time(),
        }

    accounts = db.session.execute(text("""
        SELECT id, name, kind
          FROM bud_account
         WHERE user_id = :uid AND is_active = 1 AND COALESCE(is_hidden, false) = false
         ORDER BY kind, name
    """), {"uid": current_user.id}).mappings().all()

    return render_template(
        "program_budget/import.html",
        accounts=accounts,
        preview=True,
        import_id=import_id,
        target_account_id=target_aid,
        target_account_name=target_acc_name,
        valid_count=valid_count,
        duplicate_count=duplicate_count,
        invalid_count=invalid_count,
        excluded_count=excluded_count,
        tokens_required=tokens_required,
        wallet_balance=wallet_balance,
        insufficient_tokens=(wallet_balance < tokens_required),
    )

@csrf.exempt
@budget_bp.get("/summary")
@login_required
def summary():
    period = (request.args.get("period") or "").strip() or None
    data = budget_summary(current_user.id, period=period)
    return jsonify({"ok": True, **data})

@budget_bp.get("/import")
@login_required
def import_page():
    accounts = db.session.execute(text("""
        SELECT id, name, kind
          FROM bud_account
         WHERE user_id = :uid AND is_active = 1 AND COALESCE(is_hidden, false) = false
         ORDER BY kind, name
    """), {"uid": current_user.id}).mappings().all()
    return render_template("program_budget/import.html", accounts=accounts)

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
    from app.models.auth import AitTokenWallet
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    return render_template("program_budget/dashboard.html", wallet=wallet)

@budget_bp.route("/wallet", methods=["GET"])
@budget_bp.route("/wallet/dashboard", methods=["GET"])
@login_required
def wallet_dashboard():
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    transactions = []
    if wallet:
        transactions = (
            AitTokenTransaction.query.filter_by(wallet_id=wallet.id)
            .order_by(AitTokenTransaction.created_at.desc(), AitTokenTransaction.id.desc())
            .all()
        )
    complimentary_tokens = sum(t.amount for t in transactions if t.amount > 0 and any(w in (t.description or "").lower() for w in ("welcome", "trial", "bonus", "registration")))
    purchased_tokens = sum(t.amount for t in transactions if t.amount > 0 and any(w in (t.description or "").lower() for w in ("top-up", "topup", "paystack", "purchase")))
    consumed_tokens = abs(sum(t.amount for t in transactions if t.amount < 0))

    return render_template(
        "program_budget/wallet.html",
        wallet=wallet,
        transactions=transactions,
        complimentary_tokens=complimentary_tokens,
        purchased_tokens=purchased_tokens,
        consumed_tokens=consumed_tokens,
    )

@budget_bp.get("/next")
@login_required
def next_step():
    return redirect(url_for("budget_bp.import_page"))

def _require_owned_account(account_id):
    account = db.session.execute(text(
        "SELECT id FROM bud_account WHERE id=:aid AND user_id=:uid"
    ), {"aid": int(account_id), "uid": current_user.id}).scalar()
    if account is None:
        abort(404)


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
        JOIN bud_account a ON a.id = l.account_id AND a.user_id = :uid
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
        submission_id=secrets.token_hex(16),
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

        _require_owned_account(account_id)

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
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    from app.models.billing import TokenTariff
    
    account_id = (request.form.get("account_id") or "").strip()
    txn_date = (request.form.get("txn_date") or "").strip()
    amount = (request.form.get("amount") or "").strip()
    submission_id = (request.form.get("submission_id") or request.headers.get("X-Submission-Id") or request.headers.get("Idempotency-Key") or "").strip()

    next_url = url_for(
        "budget_bp.ledger",
        account_id=account_id if account_id.isdigit() else None,
        next=_safe_next(request.args.get("next")) or url_for("budget_bp.dashboard")
    )

    if not (account_id.isdigit() and txn_date and amount):
        flash("Please choose an account, date, and amount.", "warning")
        return redirect(next_url)

    _require_owned_account(account_id)

    try:
        cents = int(round(float(amount.replace(",", "")) * 100))
    except Exception:
        flash("Invalid amount.", "warning")
        return redirect(next_url)

    # 1. Advisory transaction lock to serialize submissions per user (prevents double-click races)
    try:
        db.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
            {"k": f"budget-ledger-add:{current_user.id}"}
        )
    except Exception:
        pass

    # 2. Server-side idempotency check (under lock to protect against concurrent double-clicks)
    is_duplicate = False
    in_flight_key = (current_user.id, submission_id) if submission_id else (current_user.id, int(account_id), str(txn_date), int(cents))

    with _SUBMISSION_LOCK:
        if in_flight_key in _IN_FLIGHT_SUBMISSIONS:
            is_duplicate = True
        elif submission_id and _is_submission_processed(current_user.id, submission_id):
            is_duplicate = True
        elif not submission_id and _is_recent_duplicate(current_user.id, int(account_id), txn_date, int(cents)):
            is_duplicate = True
        else:
            _IN_FLIGHT_SUBMISSIONS.add(in_flight_key)

    if is_duplicate:
        flash("Entry already recorded.", "info")
        return redirect(next_url)

    try:
        # 3. Atomic wallet check and deduction
        with db.session.no_autoflush:
            wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).populate_existing().with_for_update().first()
            
            token_cost = 1
            try:
                tariff = TokenTariff.query.filter_by(program_slug='budget', action_name='ledger_entry').first()
                if not tariff:
                    tariff = TokenTariff(program_slug='budget', action_name='ledger_entry', base_token_cost=1)
                    db.session.add(tariff)
                    db.session.commit()
                elif tariff.base_token_cost != 1:
                    tariff.base_token_cost = 1
                    db.session.commit()
                if tariff and tariff.base_token_cost is not None:
                    token_cost = tariff.base_token_cost
            except Exception:
                token_cost = 1

            if not wallet or wallet.balance < token_cost:
                flash("Insufficient tokens. Purchase more tokens to continue.", "error")
                return redirect(url_for("payment_bp.wallet_topup", subject_slug="budget"))

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
                
                # Deduct tokens atomically in the same transaction
                wallet.balance -= token_cost
                txn = AitTokenTransaction(
                    wallet_id=wallet.id,
                    amount=-token_cost,
                    description=f"Captured Cashbook Entry (Account ID {account_id})"
                )
                db.session.add(txn)
                
                db.session.commit()

                if submission_id:
                    _mark_submission_processed(current_user.id, submission_id)
                _record_recent_submission(current_user.id, int(account_id), txn_date, int(cents))

                flash(f"Payment added. {token_cost} token{'s' if token_cost != 1 else ''} deducted.", "success")
            except Exception:
                db.session.rollback()
                flash("Could not add entry.", "warning")
    finally:
        with _SUBMISSION_LOCK:
            _IN_FLIGHT_SUBMISSIONS.discard(in_flight_key)

    return redirect(next_url)

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

    _require_owned_account(account_id)

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

        db.session.execute(text("""
        INSERT INTO bud_snapshot (user_id, account_id, as_at, arrears_cents, balance_cents, due_cents)
        VALUES (:uid, :aid, :as_at, :arrears, :balance, :due)
        """), {
        "uid": int(current_user.id),
        "aid": int(account_id),
        "as_at": as_at,
        "arrears": arrears_cents,
        "balance": balance_cents,
        "due": due_cents,
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

        if group_label:
            db.session.execute(text("""
                INSERT INTO bud_group_type (user_id, label, is_active)
                VALUES (:uid, :label, true)
                ON CONFLICT (user_id, label) DO UPDATE SET is_active = true
            """), {"uid": int(current_user.id), "label": group_label})

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

@budget_bp.route("/reports/statement", methods=["GET"])
@login_required
def report_statement():
    back_url = _safe_next(request.args.get("next")) or url_for("budget_bp.dashboard")
    period = (request.args.get("period") or "").strip()
    if not period:
        period = datetime.now().strftime("%Y-%m")

    try:
        start_date = datetime.strptime(f"{period}-01", "%Y-%m-%d").date()
    except ValueError:
        start_date = datetime.now().date().replace(day=1)
        period = start_date.strftime("%Y-%m")

    import calendar
    last_day = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = start_date.replace(day=last_day)

    # -------- INCOME --------
    income_rows = db.session.execute(text("""
        SELECT a.name, COALESCE(SUM(l.amount_cents), 0) AS cents
          FROM bud_account a
          LEFT JOIN bud_ledger l ON a.id = l.account_id AND l.user_id = :uid
                                AND l.txn_date BETWEEN :s AND :e
         WHERE a.user_id = :uid
           AND a.kind = 'income'
           AND COALESCE(a.is_hidden,false) = false
         GROUP BY a.name
         ORDER BY a.name
    """), {
        "uid": current_user.id,
        "s": start_date.strftime("%Y-%m-%d"),
        "e": end_date.strftime("%Y-%m-%d"),
    }).mappings().all()

    # -------- EXPENSES --------
    expense_rows = db.session.execute(text("""
        SELECT a.name, COALESCE(SUM(l.amount_cents), 0) AS cents
          FROM bud_account a
          LEFT JOIN bud_ledger l ON a.id = l.account_id AND l.user_id = :uid
                                AND l.txn_date BETWEEN :s AND :e
         WHERE a.user_id = :uid
           AND a.kind IN ('expense', 'liability')
           AND COALESCE(a.is_hidden,false) = false
         GROUP BY a.name
         ORDER BY a.name
    """), {
        "uid": current_user.id,
        "s": start_date.strftime("%Y-%m-%d"),
        "e": end_date.strftime("%Y-%m-%d"),
    }).mappings().all()

    income_total_cents  = sum(r["cents"] or 0 for r in income_rows)
    expense_total_cents = sum(r["cents"] or 0 for r in expense_rows)
    net_cents = income_total_cents - expense_total_cents

    # Hide accounts with 0 amounts
    income_rows = [r for r in income_rows if r["cents"] != 0]
    expense_rows = [r for r in expense_rows if r["cents"] != 0]

    
    # -------- NET WORTH --------
    accounts = db.session.execute(text("""
        SELECT id, name, kind FROM bud_account
         WHERE user_id = :uid AND is_active = 1 AND COALESCE(is_hidden,false) = false
    """), {"uid": current_user.id}).mappings().all()

    snapshots = db.session.execute(text("""
        SELECT account_id, balance_cents FROM bud_snapshot
         WHERE user_id = :uid ORDER BY as_at DESC, id DESC
    """), {"uid": current_user.id}).mappings().all()
    
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
            
        row = {"name": a["name"], "cents": bal}
        if a["kind"] in ("asset", "income"):
            asset_total_cents += bal
            asset_rows.append(row)
        else:
            # this covers "liability" and "expense"
            liability_total_cents += bal
            liability_rows.append(row)

    net_worth_cents = asset_total_cents - liability_total_cents

    return render_template(
        "program_budget/report_statement.html",
        period=period,
        income_rows=income_rows,
        expense_rows=expense_rows,
        income_total_cents=int(income_total_cents),
        expense_total_cents=int(expense_total_cents),
        net_cents=int(net_cents),
        asset_total_cents=int(asset_total_cents),
        liability_total_cents=int(liability_total_cents),
        net_worth_cents=int(net_worth_cents),
        asset_rows=asset_rows,
        liability_rows=liability_rows,
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

    snapshots = db.session.execute(text("""
        SELECT id, as_at, arrears_cents, balance_cents, due_cents, created_at
          FROM bud_snapshot
         WHERE user_id = :uid
           AND account_id = :aid
         ORDER BY created_at DESC
         LIMIT 50
    """), {"uid": int(current_user.id), "aid": int(account_id)}).mappings().all()

    return render_template(
        "program_budget/account_detail.html",
        account=account,
        paid_total_cents=int(paid_total_cents),
        groups=groups,
        snapshots=snapshots,
        next_url=url_for("budget_bp.ledger"),
    )

@budget_bp.route("/groups", methods=["GET", "POST"])
@login_required
def group_types():
    next_url = request.args.get("next") or request.form.get("next")
    if request.method == "POST":
        label = (request.form.get("label") or "").strip()
        if not label:
            flash("Please enter a group name.", "warning")
            return redirect(url_for("budget_bp.group_types", next=next_url))

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

        return redirect(url_for("budget_bp.group_types", next=next_url))

    groups = db.session.execute(text("""
        SELECT id, label
          FROM bud_group_type
         WHERE user_id = :uid
           AND is_active = true
         ORDER BY label
    """), {"uid": int(current_user.id)}).mappings().all()

    return render_template("program_budget/group_types.html", groups=groups, next_url=next_url)

@budget_bp.route("/groups/delete", methods=["POST"])
@login_required
def group_type_delete():
    next_url = request.form.get("next")
    label = (request.form.get("label") or "").strip()
    if not label:
        return redirect(url_for("budget_bp.group_types", next=next_url))

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
        flash("Could not remove group.", "warning")

    return redirect(url_for("budget_bp.group_types", next=next_url))

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

        if group_label:
            db.session.execute(text("""
                INSERT INTO bud_group_type (user_id, label, is_active)
                VALUES (:uid, :label, true)
                ON CONFLICT (user_id, label) DO UPDATE SET is_active = true
            """), {"uid": int(current_user.id), "label": group_label})

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

    groups = db.session.execute(text("""
        SELECT label
          FROM bud_group_type
         WHERE user_id = :uid AND is_active = true
         ORDER BY label
    """), {"uid": current_user.id}).mappings().all()

    return render_template(
        "program_budget/account_edit.html",
        account=account,
        groups=groups,
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
    try:
        month = datetime.strptime(period + "-01", "%Y-%m-%d").date()
    except ValueError:
        month = date.today().replace(day=1)
    period = month.strftime("%Y-%m")

    # month range [start, next_month)
    y, m = month.year, month.month
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
                COALESCE(a.group_label,'') AS group_label
            FROM bud_account a
            WHERE a.user_id = :uid
            AND COALESCE(a.is_hidden,false) = false
            {where_extra}
        ),
        month_agg AS (
            SELECT l.account_id,
                COALESCE(SUM(l.amount_cents),0) AS paid_month_cents
            FROM bud_ledger l
            WHERE l.user_id = :uid
            AND l.txn_date >= :d0
            AND l.txn_date <  :d1
            GROUP BY l.account_id
        ),
        latest_snaps AS (
            SELECT account_id, arrears_cents, balance_cents, due_cents, as_at
            FROM (
                SELECT account_id, arrears_cents, balance_cents, due_cents, as_at,
                       ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY as_at DESC, id DESC) as rn
                FROM bud_snapshot
                WHERE user_id = :uid
            ) sub
            WHERE rn = 1
        )
        SELECT s.id, s.name, s.kind, s.group_label,
            COALESCE(m.paid_month_cents,0) AS paid_month_cents,
            COALESCE(sn.arrears_cents, 0) AS arrears_cents,
            COALESCE(sn.balance_cents, 0) AS balance_cents,
            COALESCE(sn.due_cents, 0) AS due_cents,
            sn.as_at
        FROM sel s
        LEFT JOIN month_agg m ON m.account_id = s.id
        LEFT JOIN latest_snaps sn ON sn.account_id = s.id
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

    # Allow access to wallet, billing, price, about
    if request.endpoint in ("budget_bp.billing", "budget_bp.price_page", "budget_bp.about", "budget_bp.wallet_dashboard"):
        return

    ent = db.session.execute(text("""
        SELECT ue.status, ue.trial_end, ue.expires_at
          FROM user_enrollment ue
          JOIN auth_subject s ON s.id = ue.subject_id
         WHERE ue.user_id = :uid
           AND s.slug = 'budget'
         LIMIT 1
    """), {"uid": current_user.id}).mappings().first()

    if not ent:
        flash("You are not enrolled in BudgetCash.", "warning")
        return redirect(url_for("auth_bp.bridge_dashboard"))

    active = False
    if ent["status"] in ("paid", "enrolled", "started", "active"):
        active = True

    if not active:
        flash("You are not enrolled in BudgetCash.", "warning")
        return redirect(url_for("auth_bp.bridge_dashboard"))

@budget_bp.route("/billing")
@login_required
def billing():
    return redirect(url_for("budget_bp.wallet_dashboard"))

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
        ("Bank", "asset"),
        ("Cash", "asset"),
        ("Savings", "asset"),
        ("Salary", "income"),
        ("Side Hustle", "income"),
        ("Other Income", "income"),
        ("Home Loan", "liability"),
        ("Vehicle Loan", "liability"),
        ("Credit Card", "liability"),
        ("Personal Loan", "liability"),
        ("Groceries", "expense"),
        ("Transport", "expense"),
        ("Lights & Water", "expense"),
        ("Entertainment", "expense"),
        ("School Fees", "expense"),
        ("Insurance", "expense"),
        ("Medical", "expense"),
        ("Clothing", "expense"),
    ]

    for name, kind in defaults:
        db.session.execute(
            text("""
                INSERT INTO bud_account (user_id, name, kind, code)
                VALUES (:uid, :name, :kind, :code)
            """),
            {"uid": user_id, "name": name, "kind": kind, "code": _slug(name)},
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

@budget_bp.route('/price')
def price_page():
    from app.models.auth import AuthSubject
    from app.models.payment import RefCountryCurrency
    from app.enrollment.logic import get_quote_for_subject_country
    from flask import session
    
    subject = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == 'budget').first()
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for('public_bp.welcome'))

    country_code = (request.args.get("country") or "").strip().upper()
    if not country_code and current_user.is_authenticated:
        # Pull from their enrollment record if they have one
        ent = db.session.execute(text("""
            SELECT ue.country_code 
              FROM user_enrollment ue
              JOIN auth_subject s ON s.id = ue.subject_id
             WHERE ue.user_id = :uid AND s.slug = 'budget'
        """), {"uid": current_user.id}).mappings().first()
        if ent and ent["country_code"]:
            country_code = ent["country_code"]

    if not country_code:
        country_code = session.get("country_code", "")

    if country_code:
        session["country_code"] = country_code

    price_ctx = {
        "has_quote": False,
        "price_id": None,
        "country_code": None,
        "local_amount": None,
        "local_currency": None,
        "estimated_zar": None,
        "fx_rate": None,
        "is_discount": False,
    }

    if country_code:
        row = get_quote_for_subject_country(subject.id, country_code)
        if row:
            price_ctx.update({
                "price_id": row.id,
                "country_code": row.country_code,
                "local_amount": row.local_amount_cents,
                "local_currency": row.local_currency,
                "estimated_zar": row.zar_amount_cents,
                "fx_rate": getattr(row, "fx_rate", None),
                "is_discount": getattr(row, "is_discount", False),
            })
            price_ctx["has_quote"] = True
        else:
            flash("No pricing found for that country yet.", "warning")

    from app.utils.country import get_active_countries
    countries = get_active_countries()

    return render_template(
        "program_budget/price.html",
        subject=subject,
        countries=countries,
        price=price_ctx,
        country_code=country_code
    )

@budget_bp.route("/snapshots", methods=["GET", "POST"])
@login_required
def snapshots():
    next_url = request.args.get("next") or url_for("budget_bp.ledger")
    if not next_url.startswith("/"):
        next_url = url_for("budget_bp.ledger")

    if request.method == "POST":
        as_at_str = (request.form.get("as_at") or "").strip()
        if not as_at_str:
            flash("Please enter a valid date for the snapshot.", "warning")
            return redirect(url_for("budget_bp.snapshots", next=next_url))
        
        try:
            as_at = datetime.strptime(as_at_str, "%Y-%m-%d").date()
        except ValueError:
            flash("Invalid date format.", "warning")
            return redirect(url_for("budget_bp.snapshots", next=next_url))

        account_id_str = (request.form.get("account_id") or "").strip()
        arr_str = (request.form.get("arrears") or "").strip()
        due_str = (request.form.get("due") or "").strip()
        bal_str = (request.form.get("balance") or "").strip()

        if not account_id_str:
            flash("Please select an account.", "warning")
            return redirect(url_for("budget_bp.snapshots", next=next_url))

        # verify account belongs to user
        acc = db.session.execute(text("SELECT id FROM bud_account WHERE id=:aid AND user_id=:uid"), {"aid": account_id_str, "uid": current_user.id}).scalar()
        if not acc:
            flash("Invalid account selected.", "warning")
            return redirect(url_for("budget_bp.snapshots", next=next_url))

        try:
            arr_cents = _to_cents(arr_str) if arr_str else 0
            due_cents = _to_cents(due_str) if due_str else 0
            bal_cents = _to_cents(bal_str) if bal_str else 0
        except ValueError:
            flash("Enter valid numeric amounts for arrears, due and balance.", "warning")
            return redirect(url_for("budget_bp.snapshots", next=next_url))

        # Delete existing snapshot for this exact date
        db.session.execute(text("""
            DELETE FROM bud_snapshot 
             WHERE user_id=:uid AND account_id=:aid AND as_at=:dt
        """), {"uid": current_user.id, "aid": acc, "dt": as_at})

        # Insert new snapshot
        db.session.execute(text("""
            INSERT INTO bud_snapshot (user_id, account_id, as_at, arrears_cents, due_cents, balance_cents)
            VALUES (:uid, :aid, :dt, :arr, :due, :bal)
        """), {
            "uid": current_user.id,
            "aid": acc,
            "dt": as_at,
            "arr": arr_cents,
            "due": due_cents,
            "bal": bal_cents
        })
        
        db.session.commit()
        flash("Saved snapshot successfully.", "success")

        return redirect(url_for("budget_bp.snapshots", next=next_url))

    # GET request
    accounts = db.session.execute(text("""
        SELECT id, name, kind
          FROM bud_account
         WHERE user_id = :uid AND is_active = 1
         ORDER BY kind, name
    """), {"uid": current_user.id}).mappings().all()

    snapshots = db.session.execute(text("""
        SELECT s.id, s.as_at, s.arrears_cents, s.due_cents, s.balance_cents,
               a.name as account_name
          FROM bud_snapshot s
          JOIN bud_account a ON a.id = s.account_id AND a.user_id = :uid
         WHERE s.user_id = :uid
         ORDER BY s.as_at DESC, a.name
    """), {"uid": current_user.id}).mappings().all()

    return render_template("program_budget/snapshots.html", 
                           accounts=accounts, 
                           snapshots=snapshots,
                           back_url=next_url)

@budget_bp.route("/snapshots/edit/<int:snapshot_id>", methods=["GET", "POST"])
@login_required
def snapshot_edit(snapshot_id):
    next_url = request.args.get("next") or url_for("budget_bp.snapshots")
    if not next_url.startswith("/"):
        next_url = url_for("budget_bp.snapshots")

    # Fetch the snapshot and ensure it belongs to the user
    snap = db.session.execute(text("""
        SELECT s.id, s.as_at, s.arrears_cents, s.due_cents, s.balance_cents,
               a.name as account_name
          FROM bud_snapshot s
          JOIN bud_account a ON a.id = s.account_id AND a.user_id = :uid
         WHERE s.id = :sid AND s.user_id = :uid
    """), {"sid": snapshot_id, "uid": current_user.id}).mappings().first()

    if not snap:
        flash("Snapshot not found.", "warning")
        return redirect(next_url)

    if request.method == "POST":
        arr_str = (request.form.get("arrears") or "").strip()
        due_str = (request.form.get("due") or "").strip()
        bal_str = (request.form.get("balance") or "").strip()

        try:
            arr_cents = _to_cents(arr_str) if arr_str else 0
            due_cents = _to_cents(due_str) if due_str else 0
            bal_cents = _to_cents(bal_str) if bal_str else 0
        except ValueError:
            flash("Enter valid numeric amounts for arrears, due and balance.", "warning")
            return redirect(url_for("budget_bp.snapshot_edit", snapshot_id=snapshot_id, next=next_url))

        db.session.execute(text("""
            UPDATE bud_snapshot
               SET arrears_cents = :arr,
                   due_cents = :due,
                   balance_cents = :bal
             WHERE id = :sid AND user_id = :uid
        """), {
            "sid": snapshot_id,
            "uid": current_user.id,
            "arr": arr_cents,
            "due": due_cents,
            "bal": bal_cents
        })
        db.session.commit()
        flash("Snapshot updated successfully.", "success")
        return redirect(next_url)

    return render_template("program_budget/snapshot_edit.html", 
                           snapshot=snap, 
                           back_url=next_url)
@budget_bp.route("/accounts/list", methods=["GET"])
@login_required
def accounts_list():
    accounts = db.session.execute(text("""
        SELECT id, name, kind, account_no, group_label, is_hidden, is_active
          FROM bud_account
         WHERE user_id = :uid
         ORDER BY kind, name
    """), {"uid": current_user.id}).mappings().all()

    return render_template("program_budget/accounts_list.html", accounts=accounts)


@budget_bp.post('/accounts/restore-defaults')
@login_required
def restore_defaults():
    defaults = [
        ("Bank", "asset"),
        ("Cash", "asset"),
        ("Savings", "asset"),
        ("Salary", "income"),
        ("Side Hustle", "income"),
        ("Other Income", "income"),
        ("Home Loan", "liability"),
        ("Vehicle Loan", "liability"),
        ("Credit Card", "liability"),
        ("Personal Loan", "liability"),
        ("Groceries", "expense"),
        ("Transport", "expense"),
        ("Lights & Water", "expense"),
        ("Entertainment", "expense"),
        ("School Fees", "expense"),
        ("Insurance", "expense"),
        ("Medical", "expense"),
        ("Clothing", "expense"),
    ]
    
    existing_accounts = db.session.execute(
        text("SELECT lower(name) as lname FROM bud_account WHERE user_id = :uid"),
        {"uid": current_user.id}
    ).mappings().all()
    
    existing_names = set([r["lname"] for r in existing_accounts])
    
    added_count = 0
    for name, kind in defaults:
        if name.lower() not in existing_names:
            db.session.execute(
                text("""
                    INSERT INTO bud_account (user_id, name, kind, code)
                    VALUES (:uid, :name, :kind, :code)
                """),
                {"uid": current_user.id, "name": name, "kind": kind, "code": _slug(name)},
            )
            added_count += 1
            
    if added_count > 0:
        db.session.commit()
        flash(f"Restored {added_count} default account(s).", "success")
    else:
        flash("You already have all the default accounts.", "info")
        
    return redirect(url_for("budget_bp.accounts_list"))

@budget_bp.route("/support", methods=["GET", "POST"])
@login_required
def support_request():
    if request.method == "POST":
        subject = (request.form.get("subject") or "").strip()
        message = (request.form.get("message") or "").strip()
        
        if not subject or not message:
            flash("Please provide both a subject and a message.", "warning")
            return redirect(url_for("budget_bp.support_request"))
            
        try:
            from flask_mail import Message
            from app.extensions import mail
            from flask import current_app
            
            user_info = f"User: {current_user.name} ({current_user.email})\n\n"
            full_message = user_info + message
            
            msg = Message(
                subject=f"[BudgetCash Support] {subject}",
                recipients=[current_app.config.get("CONTACT_TO_EMAIL", "ait@mathwithhands.com")],
                body=full_message,
                reply_to=current_user.email
            )
            mail.send(msg)
            flash("Your support request has been sent! We will respond within 24 hours.", "success")
            return redirect(url_for("budget_bp.dashboard"))
        except Exception as e:
            flash(f"Failed to send message: {str(e)}", "warning")
            return redirect(url_for("budget_bp.support_request"))
            
    return render_template("program_budget/support.html")
