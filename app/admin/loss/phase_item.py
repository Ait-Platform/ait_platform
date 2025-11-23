# app/admin/loss/phase_item.py
from decimal import Decimal, ROUND_FLOOR
from sqlalchemy import text
from app.extensions import db
from typing import Dict, List, Tuple
from app.models import LcaOverallItem


# ------------------------------------------------------------
# Percent → item-count rules
#   Phases 1–2: 1 item per 11.1%
#   Phases 3–4: 1 item per 12.5%
# ------------------------------------------------------------

PHASE_ITEM_STEP = {
    1: Decimal("11.1"),
    2: Decimal("11.1"),
    3: Decimal("12.5"),
    4: Decimal("12.5"),
}

def _safe_pct(pct: float | int | str | None) -> Decimal:
    try:
        d = Decimal(str(pct))
    except Exception:
        d = Decimal("0")
    if d < 0:
        d = Decimal("0")
    if d > 100:
        d = Decimal("100")
    return d

def phase_item_count_for_percent(phase_no: int, pct: float | int | str) -> int:
    """
    Returns floor(pct/step) using phase-specific step.
    Example:
      phase 1, pct 99 -> floor(99/11.1)=9 items (assuming there are 9 active rows)
      phase 3, pct 75 -> floor(75/12.5)=6 items
    """
    step = PHASE_ITEM_STEP.get(int(phase_no), Decimal("12.5"))
    d_pct = _safe_pct(pct)
    return int((d_pct / step).quantize(Decimal("1"), rounding=ROUND_FLOOR))

def fetch_phase_items(phase_no: int, pct: float | int | str) -> List[str]:
    """
    Returns up to N comment 'body' rows for given phase, ordered by ordinal,id.
    N = floor(pct/step) per rules above. Only active=1 rows are returned.
    """
    n = phase_item_count_for_percent(phase_no, pct)
    if n <= 0:
        return []

    sql = text("""
        SELECT body
        FROM lca_phase_item
        WHERE phase_id = :ph AND active = 1
        ORDER BY ordinal ASC, id ASC
        LIMIT :n
    """)
    rows = db.session.execute(sql, {"ph": phase_no, "n": n}).fetchall()
    return [r[0] for r in rows]

def band_for_pct(pct: float | int | str) -> int:
    """
    1 = 0..39.999
    2 = 40..69.999
    3 = 70..100
    """
    d = _safe_pct(pct)
    if d < 40:
        return 1
    if d < 70:
        return 2
    return 3

def fetch_progress_items(phase_no: int, pct: float | int | str) -> List[str]:
    """
    Returns progress bullets for the band derived from pct.
    Expects table: lca_progress_item(phase_id, band, ordinal, body, active)
    """
    b = band_for_pct(pct)
    sql = text("""
        SELECT body
        FROM lca_progress_item
        WHERE phase_id = :ph AND band = :b AND active = 1
        ORDER BY ordinal ASC, id ASC
    """)
    rows = db.session.execute(sql, {"ph": phase_no, "b": b}).fetchall()
    return [r[0] for r in rows]

def build_phase_blocks(phase_1_pct, phase_2_pct, phase_3_pct, phase_4_pct) -> List[Dict]:
    items = [(1, phase_1_pct), (2, phase_2_pct), (3, phase_3_pct), (4, phase_4_pct)]
    blocks: List[Dict] = []
    for ph, pct in items:
        w = _width_pct(pct)
        blocks.append({
            "phase": ph,
            "pct": w,                 # normalized 0..100 (use this for labels if you want)
            "width_pct": w,           # explicit field for CSS width
            "band": band_for_pct(pct),
            "comments_list": fetch_phase_items(ph, pct),
            "progress_items": fetch_progress_items(ph, pct),
        })
    return blocks

# Add near the other helpers
def _width_pct(pct) -> int:
    try:
        n = int(float(str(pct)))
    except Exception:
        n = 0
    if n < 0: n = 0
    if n > 100: n = 100
    return n

#   <40 => low, 40..69 => mid, 70..100 => high
BAND_T1 = 40
BAND_T2 = 70

def _safe_pct(p):
    try:
        d = float(str(p))
    except Exception:
        d = 0.0
    return max(0.0, min(100.0, d))

def band_label_for_pct(pct: float) -> str:
    p = _safe_pct(pct)
    if p < BAND_T1:  return "low"
    if p < BAND_T2:  return "mid"
    return "high"

def band_for_pct(pct: float) -> int:
    # numeric band for chips: 1/2/3
    p = _safe_pct(pct)
    if p < BAND_T1:  return 1
    if p < BAND_T2:  return 2
    return 3

# -------- Comments: items-per-% --------
PHASE_ITEM_STEP = {1: 11.1, 2: 11.1, 3: 12.5, 4: 12.5}

def phase_item_count_for_percent(phase_no: int, pct) -> int:
    step = PHASE_ITEM_STEP.get(int(phase_no), 12.5)
    return int(_safe_pct(pct) // step)

def fetch_phase_items(phase_no: int, pct) -> List[str]:
    n = phase_item_count_for_percent(phase_no, pct)
    if n <= 0:
        return []
    rows = db.session.execute(
        text("""
            SELECT body
            FROM lca_phase_item
            WHERE phase_id = :ph AND active = 1
            ORDER BY ordinal ASC, id ASC
            LIMIT :n
        """),
        {"ph": phase_no, "n": n},
    ).fetchall()
    return [r[0] for r in rows]

# -------- Progress: notes by (phase, band_label) --------
# Table you provided: lca_progress_item(id, phase_id, band, tone, body, ordinal, active)
TONE_CHIP = {
    "positive":           "bg-emerald-50 text-emerald-700 border-emerald-200",
    "slightly_positive":  "bg-amber-50 text-amber-700 border-amber-200",
    "negative":           "bg-rose-50 text-rose-700 border-rose-200",
}

def fetch_progress_notes(phase_no: int, pct) -> Dict:
    band_label = band_label_for_pct(_safe_pct(pct))  # "low" | "mid" | "high"

    rows = db.session.execute(
        text("""
            SELECT tone, body
            FROM lca_progress_item
            WHERE phase_id = :ph
            AND band = :band
            AND COALESCE(active, TRUE) = TRUE
            ORDER BY ordinal ASC, id ASC
        """),
        {"ph": phase_no, "band": band_label},
    ).fetchall()



    if not rows:
        return {"band": band_label, "tone": None, "chip_class": "", "notes": []}

    tone = rows[0][0] or ""
    chip = TONE_CHIP.get(tone, "bg-slate-50 text-slate-700 border-slate-200")
    notes = [r[1] for r in rows]
    return {"band": band_label, "tone": tone, "chip_class": chip, "notes": notes}

# -------- Colors for phase bars/chips (orientation-aware) --------
def level_from_band(band: int) -> str:
    return "High" if band == 3 else ("Medium" if band == 2 else "Low")

def color_classes_for(phase_no: int, pct) -> Tuple[str, str]:
    """
    Returns (bar_bg_class, badge_chip_classes) such that:
      Phases 1–2: higher == worse  -> low=green, mid=amber, high=red
      Phases 3–4: higher == better -> low=red,   mid=amber, high=green
    """
    p = _safe_pct(pct)
    if phase_no in (1, 2):
        if p >= BAND_T2:  return ("bg-rose-600",   "bg-rose-50 text-rose-700 border-rose-200")
        if p >= BAND_T1:  return ("bg-amber-500",  "bg-amber-50 text-amber-700 border-amber-200")
        return ("bg-emerald-600",                  "bg-emerald-50 text-emerald-700 border-emerald-200")
    else:
        if p >= BAND_T2:  return ("bg-emerald-600","bg-emerald-50 text-emerald-700 border-emerald-200")
        if p >= BAND_T1:  return ("bg-amber-500",  "bg-amber-50 text-amber-700 border-amber-200")
        return ("bg-rose-600",                     "bg-rose-50 text-rose-700 border-rose-200")

# -------- Adaptive Vector (overall) --------
def adaptive_vector_from_phases(p1, p2, p3, p4) -> str:
    """
    Simple, explainable rule:
      Coping            if p1<40 and p2<40 and p3>=70 and p4>=70
      Slightly Coping   if (p3>=40 or p4>=40) and (p1<70 and p2<70)
      Not Coping        otherwise
    """
    a, b, c, d = _safe_pct(p1), _safe_pct(p2), _safe_pct(p3), _safe_pct(p4)
    if (a < 40 and b < 40 and c >= 70 and d >= 70):  return "Coping"
    if ((c >= 40 or d >= 40) and (a < 70 and b < 70)):  return "Slightly Coping"
    return "Not Coping"

def overall_assessment_from_p1(p1_pct) -> dict:
    """
    Overall Assessment is based ONLY on Phase 1, collapsed to two buckets:
      - LOW  (0–39)  -> comment 1 (placeholder 1)
      - HIGH (≥40)   -> comment 2 (placeholder 2)   # medium is treated as 'high' per 2-aspect rule
    Returns a dict safe for the template.
    """
    p = _safe_pct(p1_pct)
    is_low = p < 40.0
    label = "Low" if is_low else "High"
    comment = "placeholder 1" if is_low else "placeholder 2"
    chip_class = (
        "bg-emerald-50 text-emerald-700 border-emerald-200" if is_low
        else "bg-rose-50 text-rose-700 border-rose-200"
    )
    return {"label": label, "comment": comment, "pct": int(p), "chip_class": chip_class}

#from .colors import color_classes_for_map

# === Phase×Band → Tailwind classes ===
BAR_CLASS = {
    1: {"low": "bg-emerald-600", "mid": "bg-amber-500",  "high": "bg-rose-600"},  # higher = worse
    2: {"low": "bg-emerald-600", "mid": "bg-amber-500",  "high": "bg-rose-600"},  # higher = worse
    3: {"low": "bg-emerald-600", "mid": "bg-amber-500",  "high": "bg-rose-600"},  # higher = worse
    4: {"low": "bg-blue-600",    "mid": "bg-emerald-400","high": "bg-emerald-600"},# higher = better
}

CHIP_CLASS = {
    1: {
        "low":  "bg-emerald-50 text-emerald-700 border-emerald-200",
        "mid":  "bg-amber-50   text-amber-700   border-amber-200",
        "high": "bg-rose-50    text-rose-700    border-rose-200",
    },
    2: {
        "low":  "bg-emerald-50 text-emerald-700 border-emerald-200",
        "mid":  "bg-amber-50   text-amber-700   border-amber-200",
        "high": "bg-rose-50    text-rose-700    border-rose-200",
    },
    3: {
        "low":  "bg-emerald-50 text-emerald-700 border-emerald-200",
        "mid":  "bg-amber-50   text-amber-700   border-amber-200",
        "high": "bg-rose-50    text-rose-700    border-rose-200",
    },
    4: {
        "low":  "bg-blue-50    text-blue-700    border-blue-200",
        "mid":  "bg-emerald-50 text-emerald-700 border-emerald-200",  # light-green vibe for mid
        "high": "bg-emerald-50 text-emerald-700 border-emerald-200",
    },
}

def color_classes_for_map(phase_no: int, pct) -> tuple[str, str]:
    band = band_label_for_pct(pct)  # "low" | "mid" | "high"
    return BAR_CLASS[int(phase_no)][band], CHIP_CLASS[int(phase_no)][band]

def build_phase_blocks(p1, p2, p3, p4) -> list[dict]:
    items = [(1, p1), (2, p2), (3, p3), (4, p4)]
    blocks = []
    for ph, pct in items:
        w = _width_pct(pct)
        band_num = band_for_pct(pct)               # 1/2/3 if you still use it
        bar_cls, chip_cls = color_classes_for_map(ph, pct)   # ← uses the maps
        progress = fetch_progress_notes(ph, pct)   # unchanged

        blocks.append({
            "phase": ph,
            "pct": w,
            "width_pct": w,
            "band": band_num,
            "bar_class": bar_cls,          # ← for the progress bar fill
            "badge_class": chip_cls,       # ← for chips (Impact / AV / Progress band)
            "level": level_from_band(band_num),
            "coping": ("Coping" if (ph in (3,4) and w>=70) or (ph in (1,2) and w<40)
                       else "Slightly Coping" if (40<=w<70) else "Not Coping"),
            "comments_list": fetch_phase_items(ph, pct),
            "progress": progress,
        })
    return blocks




def overall_assessment_from_p1(p1_pct) -> dict:
    """
    Build the 'Overall Assessment' block from lca_overall_item.
    Uses 'low' if p1_pct < 40 else 'high'.
    Falls back to legacy copy if DB has no rows.
    """
    def _safe_pct(x):
        try:
            v = float(x)
        except Exception:
            return 0.0
        return max(0.0, min(100.0, v))

    p = _safe_pct(p1_pct)
    band = "low" if p < 40.0 else "high"
    pct_i = int(p)

    CHIP = {
        "low":  "bg-emerald-50 text-emerald-700 border-emerald-200",
        "high": "bg-rose-50 text-rose-700 border-rose-200",
    }

    # ---- Fetch from DB
    q_base = (db.session.query(LcaOverallItem)
              .filter(LcaOverallItem.band == band,
                      LcaOverallItem.active.is_(True)))

    summary = (q_base.filter(LcaOverallItem.type == "summary")
                     .order_by(LcaOverallItem.ordinal.asc())
                     .first())
    bullets = (q_base.filter(LcaOverallItem.type == "bullet")
                     .order_by(LcaOverallItem.ordinal.asc())
                     .all())

    if summary:
        return {
            "label":    summary.label or band.title(),
            "pct":      pct_i,
            "chip_class": CHIP.get(band, ""),
            "summary":  summary.body,
            "bullets":  [b.body for b in bullets],
            "key_need": summary.key_need,
        }

    # ---- Fallback (your current text) if table isn’t populated
    if band == "low":
        return {
            "label": "Low",
            "pct": pct_i,
            "chip_class": CHIP["low"],
            "summary": (
                "You’re carrying this experience with steadiness. Keep what’s working—simple routines, "
                "small connections, and naming needs—so your footing stays firm as you move forward."
            ),
            "bullets": [],
            "key_need": None,
        }

    # High fallback
    return {
        "label": "High",
        "pct": pct_i,
        "chip_class": CHIP["high"],
        "summary": (
            "It looks like this event is still taking up a lot of space, which can tug anyone into a place "
            "where adjusting feels hard. The good news is you’re not stuck—most people regain their footing "
            "with a little gentle structure and reliable support."
        ),
        "bullets": [
            "Anchor one small daily routine (wake → eat → move → rest).",
            "Reconnect with one trusted person this week.",
            "Name one feeling + one need each day to lower overload.",
            "Keep a brief “worry window” (e.g., 10 minutes) to reduce rumination.",
            "Use a calming reset (slow exhale breathing, short walk, stretch).",
            "If distress stays high or safety is a concern, speak to a counselor/GP.",
        ],
        "key_need": "gentle structure + reliable support",
    }


DISCLAIMER_TEXT = (
    "This report is an informational summary to support reflection and planning. "
    "It is not a diagnosis or a substitute for professional care. If you are in distress "
    "or worried about safety, please contact a health professional or local emergency services."
)
