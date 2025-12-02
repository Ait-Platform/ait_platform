# payments/AuthPricing.py
from decimal import ROUND_HALF_UP, Decimal
import requests
from sqlalchemy import select, and_, or_, func, text
from app.extensions import db
from flask import current_app, g, redirect, request, session, url_for
from datetime import datetime, timezone
from app.models.auth import AuthSubject, AuthPricing
from app.models.payment import RefCountryCurrency
from app.subject_reading.routes import _ensure_enrollment_row
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

VAT_RATE = 0.15  # SA VAT

def get_subject_price(subject_slug: str, role: str = "learner", plan: str = "enrollment"):
    """Return the active price row for a subject slug, with VAT-inclusive display."""
    now = datetime.utcnow()

    subj = db.session.execute(
        select(AuthSubject.id).where(AuthSubject.slug == subject_slug)
    ).scalar_one_or_none()
    if not subj:
        return None  # unknown subject

    row = db.session.execute(
        select(AuthPricing)
        .where(
            and_(
                AuthPricing.subject_id == subj,
                AuthPricing.role == role,
                AuthPricing.plan == plan,
                AuthPricing.is_active == True,
                or_(AuthPricing.active_from == None, AuthPricing.active_from <= now),
                or_(AuthPricing.active_to == None, AuthPricing.active_to > now),
            )
        )
        .order_by(AuthPricing.active_from.desc().nulls_last())
        .limit(1)
    ).scalar_one_or_none()

    if not row:
        return None

    # build a tiny view model
    amount = (row.amount_cents or 0) / 100.0
    total = round(amount * (1 + VAT_RATE), 2)
    currency = (row.currency or "ZAR").upper()
    # simple ZAR display
    display = f"R {total:,.2f}" if currency == "ZAR" else f"{currency} {total:,.2f}"

    return {
        "subject_id": row.subject_id,
        "currency": currency,
        "amount_cents": row.amount_cents,
        "amount_ex_vat": amount,
        "amount_incl_vat": total,
        "display": display,
        "vat_rate": VAT_RATE,
    }


# app/utils/text_format.py
def _chunk_to_words(n: int) -> str:
    ones = ["zero","one","two","three","four","five","six","seven","eight","nine",
            "ten","eleven","twelve","thirteen","fourteen","fifteen","sixteen","seventeen","eighteen","nineteen"]
    tens = ["","", "twenty","thirty","forty","fifty","sixty","seventy","eighty","ninety"]

    if n < 20:
        return ones[n]
    if n < 100:
        t, r = divmod(n, 10)
        return tens[t] + (f"-{ones[r]}" if r else "")
    h, r = divmod(n, 100)
    return ones[h] + " hundred" + (f" and {_chunk_to_words(r)}" if r else "")

def number_to_words(n: int) -> str:
    if n == 0:
        return "Zero"
    if n < 0:
        return "Minus " + number_to_words(-n)

    words = []
    thousands, rem = divmod(n, 1000)
    if thousands:
        words.append(_chunk_to_words(thousands) + " thousand")
    if rem:
        words.append(_chunk_to_words(rem))
    s = " ".join(words)
    return s[:1].upper() + s[1:]


def detect_country(request) -> str | None:
    # Cloudflare adds this for free
    cc = request.headers.get("CF-IPCountry")
    if cc and len(cc) == 2: return cc.upper()
    return None


__all__ = ["price_cents_for", "price_dict_for"]

def price_cents_for(subject_slug: str, currency: str = "ZAR") -> int | None:
    """
    Return the active price (in cents) for a subject + currency, or None if none found.
    """
    row = db.session.execute(
        """
        SELECT p.amount_cents
        FROM auth_pricing p
        JOIN auth_subject s ON s.id = p.subject_id
        WHERE s.slug = :slug
          AND p.currency = :cur
          AND p.is_active = 1
          AND (p.active_from IS NULL OR p.active_from <= CURRENT_TIMESTAMP)
          AND (p.active_to   IS NULL OR p.active_to   >  CURRENT_TIMESTAMP)
        ORDER BY p.active_from DESC
        LIMIT 1
        """,
        {"slug": subject_slug, "cur": currency},
    ).fetchone()
    return int(row[0]) if row else None


def price_dict_for(subject_slug: str, currency: str = "ZAR") -> dict | None:
    """
    Convenience wrapper for templates: returns {"currency": "...", "amount_cents": N} or None.
    """
    cents = price_cents_for(subject_slug, currency)
    return {"currency": currency, "amount_cents": cents} if cents is not None else None

FALLBACK_PRICE_CENTS = 5000  # display only; real charge logic can differ

def start_enrollment(user_id: int, subject_slug: str):
    country = detect_country(request) or request.form.get("country") or "ZA"
    currency, amount_cents, price_version = price_for_country(subject_slug, country)

    ue = _ensure_enrollment_row(user_id, subject_slug)  # your existing helper
    ue.country_code = country
    ue.quoted_currency = currency
    ue.quoted_amount_cents = amount_cents
    ue.price_version = price_version
    ue.price_locked_at = datetime.utcnow()
    db.session.commit()
    return redirect(url_for("auth_bp.review_and_pay", subject=subject_slug))

# DB lookup first
def _db_currency_for(code: str) -> str | None:
    if not code:
        return None
    row = db.session.execute(
        sa.text("SELECT currency FROM ref_country_currency WHERE alpha2 = :c LIMIT 1"),
        {"c": (code or "").upper()},
    ).first()
    return row.currency if row and row.currency else None

# Optional fallback to country_list entries if you later add {"currency": "ZAR"}
def _list_currency_for(code: str) -> str | None:
    try:
        from utils.country_list import COUNTRIES
    except Exception:
        return None
    c = (code or "").upper()
    for item in COUNTRIES:
        if isinstance(item, dict):
            code2 = str(item.get("code") or item.get("alpha2") or item.get("iso") or "").upper()
            curr  = str(item.get("currency") or item.get("ccy") or "").upper()
            if code2 == c and curr:
                return curr
    return None


# 1) Read active anchor from auth_pricing (in cents)
def get_parity_anchor_cents(subject_id: int) -> int:
    row = db.session.execute(
        db.text("""
            SELECT amount_cents
            FROM auth_pricing
            WHERE subject_id = :sid
              AND plan = 'enrollment'
              AND is_active = 1
              AND active_from <= CURRENT_TIMESTAMP
              AND (active_to IS NULL OR active_to > CURRENT_TIMESTAMP)
            ORDER BY active_from DESC, updated_at DESC
            LIMIT 1
        """),
        {"sid": subject_id},
    ).first()
    return int(row.amount_cents or 0) if row else 0

# 2) Country → (name, code) using your utils.country_list
def resolve_country_name_code(user_input: str) -> tuple[str, str]:
    from utils.country_list import resolve_country, _name_code_iter, COUNTRIES
    name = resolve_country(user_input or "")
    code = ""
    nl = name.lower()
    for nm, cd in _name_code_iter(COUNTRIES):
        if nm.lower() == nl:
            code = (cd or "").upper()
            break
    ui = (user_input or "").strip()
    if not code and len(ui) == 2 and ui.isalpha():
        code = ui.upper()
    return name, code

# 3) Currency lookup (pure data: ref_country_currency model)

# 4) Session lock: country + display currency + parity value
def lock_country_and_price(session, user_input_country: str, subject_id: int):
    name, code = resolve_country_name_code(user_input_country)
    cents = get_parity_anchor_cents(subject_id)
    session["pp_country"] = code or "ZA"
    session["pp_country_name"] = name
    session["pp_currency"] = currency_for_country_code(code)      # display-only
    session["pp_value"] = round((cents or 0) / 100.0, 2)          # parity anchor
    session["pp_discount"] = False
    session["pp_vat_note"] = "excl. VAT"

# Subject id from slug (e.g., "loss")
# Subject id from slug (e.g., "loss")
def subject_id_for(slug: str) -> int | None:
    row = db.session.execute(
        db.text("SELECT id FROM auth_subject WHERE slug = :s LIMIT 1"),
        {"s": slug},
    ).first()
    return int(row.id) if row else None

# Table-driven parity price for a country (returns currency, amount_cents, source)


def apply_percentage_discount(sess, pct: float) -> None:
    """
    Apply a one-time percentage discount to pp_value.
    - Only applies once (pp_discount flag)
    - Clamps to a minimum of 0.05
    """
    # already discounted? bail
    if sess.get("pp_discount"):
        return

    try:
        val = float(sess.get("pp_value") or 0.0)
    except (TypeError, ValueError):
        return

    if val <= 0:
        return

    try:
        pct = float(pct or 0.0)
    except (TypeError, ValueError):
        pct = 0.0

    if pct <= 0:
        return

    new_val = round(val * (1.0 - pct / 100.0), 2)

    # clamp to minimum 0.05
    if new_val < 0.05:
        new_val = 0.05

    sess["pp_value"] = new_val
    sess["pp_discount"] = True

# payments/pricing.py

def countries_from_ref() -> list[dict]:
    rows = db.session.execute(
        db.text("SELECT alpha2, currency FROM ref_country_currency ORDER BY alpha2")
    ).fetchall()
    # If you don't want names, show codes; totally DB-driven
    return [{"code": r.alpha2, "label": r.alpha2, "currency": r.currency} for r in rows]

def countries_from_ref_with_names() -> list[dict]:
    rows = db.session.execute(
        db.text("SELECT alpha2, currency FROM ref_country_currency ORDER BY alpha2")
    ).fetchall()

    # name lookup only (for label); currency still comes from DB
    try:
        from app.utils.country_list import _name_code_iter, COUNTRIES
    except ImportError:
        from utils.country_list import _name_code_iter, COUNTRIES

    code_to_name = { (cd or "").upper(): nm for nm, cd in _name_code_iter(COUNTRIES) }
    return [{"code": r.alpha2.upper(),
             "name": code_to_name.get(r.alpha2.upper(), r.alpha2.upper()),
             "currency": r.currency} for r in rows]

def currency_for_country_code(code: str) -> str | None:
    """
    Returns 3-letter currency code for a 2-letter country code, e.g. 'AU' -> 'AUD'.
    """
    row = db.session.execute(
        text("SELECT currency FROM ref_country_currency WHERE alpha2 = :c"),
        {"c": code},
    ).fetchone()
    return row[0] if row else None

# Make sure this module imports: datetime, requests, text from sqlalchemy

FX_MAX_AGE_HOURS = 24

def fx_rate_local_to_zar(country_code: str) -> float | None:
    """
    Returns fx such that: 1 unit of local currency * fx = N ZAR.
    Uses ref_country_currency.fx_to_zar as a cache.
    Falls back to live API if missing/stale.
    Returns None if we cannot get any rate at all.
    """
    # 1) Get currency for country (e.g. 'AE' -> 'AED')
    cur = currency_for_country_code(country_code) or "ZAR"
    if cur.upper() == "ZAR":
        return 1.0

    # 2) Try cached value
    row = db.session.execute(
        text("""
            SELECT fx_to_zar, updated_at
            FROM ref_country_currency
            WHERE alpha2 = :c
            LIMIT 1
        """),
        {"c": country_code},
    ).mappings().first()

    now = datetime.utcnow()

    def _age_hours(ts) -> float | None:
        if not ts:
            return None
        try:
            # Postgres gives datetime, SQLite gives string
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            return (now - ts).total_seconds() / 3600.0
        except Exception:
            return None

    if row and row["fx_to_zar"] is not None:
        age = _age_hours(row["updated_at"])
        if age is not None and age <= FX_MAX_AGE_HOURS:
            # Fresh enough
            try:
                return float(row["fx_to_zar"])
            except (TypeError, ValueError):
                pass  # fall through to live fetch

    # 3) Live fetch via ExchangeRate API
    try:
        url = f"https://api.exchangerate-api.com/v4/latest/{cur}"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json() or {}
            rates = data.get("rates") or {}
            fx = rates.get("ZAR")
            if fx:
                fx_val = float(fx)

                # upsert cache
                db.session.execute(
                    text("""
                        INSERT INTO ref_country_currency (alpha2, currency, fx_to_zar, updated_at)
                        VALUES (:a2, :ccy, :fx, CURRENT_TIMESTAMP)
                        ON CONFLICT(alpha2) DO UPDATE SET
                            fx_to_zar = excluded.fx_to_zar,
                            updated_at = CURRENT_TIMESTAMP
                    """),
                    {"a2": country_code, "ccy": cur.upper(), "fx": fx_val},
                )
                db.session.commit()
                return fx_val
    except Exception as exc:
        current_app.logger.warning(f"Live FX fetch failed for {country_code}/{cur}: {exc}")

    # 4) Live failed; if we had *any* cached value, use that as a last resort
    if row and row["fx_to_zar"] is not None:
        try:
            return float(row["fx_to_zar"])
        except (TypeError, ValueError):
            pass

    # 5) No rate at all
    return None

def _resolve_subject_from_request() -> tuple[int, str]:
    """
    Decide which subject we're pricing for, based on:
      - explicit ?subject=... in the URL, or
      - reg_ctx["subject"] in the session, or
      - default 'loss'
    Returns: (subject_id, subject_slug)
    """
    slug = (request.args.get("subject") or "").strip().lower()
    if not slug:
        reg_ctx = session.get("reg_ctx") or {}
        slug = (reg_ctx.get("subject") or "loss").strip().lower()

    sid = subject_id_for(slug)
    return sid, slug


def price_for_country(subject_id, country_code):
    """
    Return a pure table-driven price:

        local_cents  – subject_country_price.local_amount_cents
        zar_cents    – subject_country_price.zar_amount_cents
        currency     – ref_country_currency.currency

    No FX maths, no ref_subject_parity_price, no COALESCE tricks.
    """
    cc = (country_code or "").strip().upper()
    if not cc:
        cc = "ZA"

    row = db.session.execute(
        text("""
            SELECT p.local_amount_cents, p.zar_amount_cents, c.currency
            FROM subject_country_price p
            JOIN ref_country_currency c
              ON c.alpha2 = p.country_code
             AND c.is_active = true
           WHERE p.subject_id   = :sid
             AND p.country_code = :cc
             AND p.is_active    = 1
           LIMIT 1
        """),
        {"sid": subject_id, "cc": cc},
    ).first()

    if not row:
        # Fallback: same subject, ZA row (still table-driven, no FX)
        row = db.session.execute(
            text("""
                SELECT p.local_amount_cents, p.zar_amount_cents, c.currency
                FROM subject_country_price p
                JOIN ref_country_currency c
                  ON c.alpha2 = p.country_code
                 AND c.is_active = true
               WHERE p.subject_id   = :sid
                 AND p.country_code = 'ZA'
                 AND p.is_active    = 1
               LIMIT 1
            """),
            {"sid": subject_id},
        ).first()

    if not row:
        # last-resort safe default
        return 0, 0, "ZAR"

    local_cents, zar_cents, cur = row
    return int(local_cents or 0), int(zar_cents or 0), (cur or "ZAR")
