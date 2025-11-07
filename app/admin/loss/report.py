# ---- add near top if not present ----
from flask import request, render_template, abort
from sqlalchemy import text
from datetime import datetime
from app import db

def _phase_maxima():
    # If you have lca_phase_maxima(phase, max_score), use it.
    try:
        rows = db.session.execute(
            text("SELECT phase, max_score FROM lca_phase_maxima")
        ).fetchall()
        if rows:
            return {int(r[0]): int(r[1] or 0) for r in rows}
    except Exception:
        pass
    # Fallback: unknown maxima -> None (percents omitted)
    return {}

def _classify_adaptive_vector(pcts: list[int]) -> str:
    # Simple, editable thresholds
    good = sum(1 for p in pcts if p >= 70)
    some = sum(1 for p in pcts if 40 <= p < 70)
    if good >= 2:
        return "Coping"
    if some >= 1:
        return "Slightly Coping"
    return "Not Coping"

# --- LOSS Report: route + helpers ---
import math

# ---------- DB fetches ----------
def _get_result_for_run(run_id: int):
    """Fetch only always-present columns; don't assume max_phase_* exists."""
    return db.session.execute(
        text("""
          SELECT id, user_id, run_id, subject, created_at,
                 phase_1, phase_2, phase_3, phase_4, total
          FROM lca_result
          WHERE run_id = :rid
        """),
        {"rid": run_id}
    ).mappings().first()

def _try_maxima_from_lca_result(run_id: int) -> dict[int, int] | None:
    """Try to read max_phase_* from lca_result if those columns exist."""
    try:
        row = db.session.execute(
            text("""
              SELECT max_phase_1, max_phase_2, max_phase_3, max_phase_4
              FROM lca_result
              WHERE run_id = :rid
            """),
            {"rid": run_id}
        ).first()
        if row and all(row[i] is not None for i in range(4)):
            return {1: int(row[0] or 0), 2: int(row[1] or 0), 3: int(row[2] or 0), 4: int(row[3] or 0)}
    except Exception:
        pass
    return None

def _phase_maxima_fallbacks(run_id: int) -> dict[int, int]:
    """
    Resolve per-phase maxima without assuming schema.
    Order:
      1) lca_result.max_phase_*
      2) lca_phase_maxima(phase,max_score)
      3) lca_scoring_map / lca_question_phase_map summed
      4) observed peak from lca_scorecard_v
      5) constants matching your example: 18,18,32,32
    """
    # 1) from lca_result if columns exist
    m = _try_maxima_from_lca_result(run_id)
    if m and sum(m.values()) > 0:
        return m

    # 2) explicit maxima table
    try:
        rows = db.session.execute(text("SELECT phase, max_score FROM lca_phase_maxima")).fetchall()
        if rows:
            m = {int(r[0]): int(r[1] or 0) for r in rows}
            if sum(m.values()) > 0:
                return m
    except Exception:
        pass

    # 3) scoring map / question map (common variants)
    for sql in [
        "SELECT phase, SUM(max_score) FROM lca_scoring_map GROUP BY phase",
        "SELECT phase, SUM(score_max) FROM lca_scoring_map GROUP BY phase",
        "SELECT phase, SUM(max_score) FROM lca_question_phase_map GROUP BY phase",
        "SELECT phase, SUM(score_max) FROM lca_question_phase_map GROUP BY phase",
    ]:
        try:
            rows = db.session.execute(text(sql)).fetchall()
            if rows:
                m = {int(r[0]): int(r[1] or 0) for r in rows}
                if sum(m.values()) > 0:
                    return m
        except Exception:
            continue

    # 4) observed peak per phase from scorecard view
    try:
        rows = db.session.execute(text("""
            SELECT phase, MAX(total_phase) AS peak
            FROM (
              SELECT run_id, phase, SUM(score) AS total_phase
              FROM lca_scorecard_v
              GROUP BY run_id, phase
            ) x
            GROUP BY phase
        """)).fetchall()
        if rows:
            m = {int(r[0]): int(r[1] or 0) for r in rows}
            if sum(m.values()) > 0:
                return m
    except Exception:
        pass

    # 5) constants (your sample row): 18 + 18 + 32 + 32 = 100
    return {1: 18, 2: 18, 3: 32, 4: 32}

# ---------- content sources ----------
def _load_phase_items() -> dict[int, list[str]]:
    """Phase items (no band): {phase_id: [body…]} ordered by ordinal."""
    rows = db.session.execute(text("""
        SELECT phase_id, body
        FROM lca_phase_item
        WHERE COALESCE(active,1)=1
        ORDER BY phase_id, COALESCE(ordinal,1)
    """)).mappings().all()
    out: dict[int, list[str]] = {}
    for r in rows:
        out.setdefault(int(r["phase_id"]), []).append(r["body"])
    return out

def _load_progress_banded() -> dict[int, dict[str, str]]:
    """Progress items (banded): {phase_id: {low|mid|high: body}}."""
    rows = db.session.execute(text("""
        SELECT phase_id, LOWER(TRIM(band)) AS band, body
        FROM lca_progress_item
        WHERE COALESCE(active,1)=1
        ORDER BY phase_id, band, COALESCE(ordinal,1)
    """)).mappings().all()
    out: dict[int, dict[str, str]] = {}
    for r in rows:
        ph = int(r["phase_id"])
        out.setdefault(ph, {})
        out[ph].setdefault(r["band"], r["body"])  # first per band wins
    return out

# ---------- rules ----------
def _pct(score: int | None, maxv: int | None) -> int | None:
    if not maxv or maxv <= 0 or score is None:
        return None
    return max(0, min(100, round((int(score) / int(maxv)) * 100)))

def _band_by_12p5(p: int | None) -> str:
    if p is None:
        return "mid"
    steps = math.floor((p + 1e-9) / 12.5)
    if steps <= 3: return "low"
    if steps <= 5: return "mid"
    return "high"

def _select_phase_comments(items: list[str], pct: int | None, *, phase: int, max_show: int | None = None) -> list[str]:
    """Phases 1–2: ⌊%/11.1⌋ items; phases 3–4: ⌊%/12.5⌋ items."""
    if not items or pct is None or pct <= 0:
        return []
    step = 11.1 if phase in (1, 2) else 12.5
    n = math.floor((pct + 1e-9) / step)
    if max_show is not None:
        n = min(n, max_show)
    n = max(0, min(n, len(items)))
    return items[:n]

def _get_user_info(user_id: int):
    try:
        row = db.session.execute(text("SELECT name, email FROM auth_user WHERE id=:uid"), {"uid": user_id}).first()
        return {"name": row[0] if row else None, "email": row[1] if row else None}
    except Exception:
        return {"name": None, "email": None}

def _adaptive_vector_from_bands(pcts: dict[int, int | None]) -> str:
    highs = sum(1 for ph in (1,2,3,4) if _band_by_12p5(pcts.get(ph)) == "high")
    mids  = sum(1 for ph in (1,2,3,4) if _band_by_12p5(pcts.get(ph)) == "mid")
    if highs >= 2: return "Not Coping"
    if mids  >= 2: return "Slightly Coping"
    return "Coping"

# ---------- view ----------
