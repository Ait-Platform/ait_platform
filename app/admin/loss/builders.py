# app/admin/loss/builders.py
from sqlalchemy import text

LOW, MID, HIGH = 1, 2, 3

def pct(score, mx):
    try:
        s = float(score or 0); m = float(mx or 0)
        return int(round(100.0 * s / m)) if m > 0 else 0
    except Exception:
        return 0

def band_for_phase(phase:int, p:int) -> int:
    # Ranges: low 0–39, mid 40–69, high 70–100 (same for all phases)
    if p >= 70: return HIGH
    if p >= 40: return MID
    return LOW

def coping_label(phase:int, band:int) -> str:
    # 1–3: low→Coping, mid→Slightly, high→Not Coping
    # 4:    low→Not Coping, mid→Slightly Coping, high→Coping
    if phase in (1,2,3):
        return "Coping" if band==LOW else ("Slightly Coping" if band==MID else "Not Coping")
    # phase 4
    return "Not Coping" if band==LOW else ("Slightly Coping" if band==MID else "Coping")

def range_label(band:int) -> str:
    return "Low" if band==LOW else ("Medium" if band==MID else "High")

def _phase_items_for_comments(db, phase:int, how_many:int):
    # Pull top N active items by phase + ordinal
    rows = db.session.execute(text("""
        SELECT body
        FROM phase_item
        WHERE active=1 AND phase_id=:p
        ORDER BY ordinal ASC
        LIMIT :n
    """), {"p": phase, "n": how_many}).mappings().all()
    return [r["body"] for r in rows]

def _progress_items_for_band(db, phase:int, band:int):
    # Dump entire group for that phase+band ordered by ordinal
    # band column stores textual: 'low' | 'mid' | 'high'
    band_txt = "low" if band==LOW else ("mid" if band==MID else "high")
    rows = db.session.execute(text("""
        SELECT body
        FROM progress_item
        WHERE active=1 AND phase_id=:p AND band=:b
        ORDER BY ordinal ASC
    """), {"p": phase, "b": band_txt}).mappings().all()
    return [r["body"] for r in rows]

def _comment_count_for_phase(phase:int, p:int) -> int:
    # Phases 1–2: 11.1% per item; Phases 3–4: 12.5% per item
    step = 11.1 if phase in (1,2) else 12.5
    # floor with a ceiling of 9/8 respectively; 99% → 9 items for p1/2; 100% → 9/8 too
    count = int(p // step)
    # safety caps (from your examples: p1/2 up to 9; p3/4 up to 8)
    cap = 9 if phase in (1,2) else 8
    return max(0, min(count, cap))

def build_phase_blocks(db, row):
    """
    row: SQLAlchemy mapping with fields
      phase_1..phase_4, max_p1..max_p4, etc.
    Returns list[dict] safe for the template.
    """
    pcts = {
        1: pct(row["phase_1"], row["max_p1"]),
        2: pct(row["phase_2"], row["max_p2"]),
        3: pct(row["phase_3"], row["max_p3"]),
        4: pct(row["phase_4"], row["max_p4"]),
    }

    blocks = []
    labels = {1:"Impact", 2:"Hopelessness", 3:"Helplessness", 4:"Re-Engagement"}

    for phase in (1,2,3,4):
        p = pcts[phase]
        band = band_for_phase(phase, p)
        comments_count = _comment_count_for_phase(phase, p)
        comments = _phase_items_for_comments(db, phase, comments_count)
        prog_items = _progress_items_for_band(db, phase, band)

        blocks.append({
            "phase": phase,
            "label": labels[phase],
            "pct": p,
            "band": band,                     # 1/2/3
            "level": range_label(band),       # Low/Medium/High
            "coping": coping_label(phase, band),
            "comments_list": comments,        # list[str]
            "progress": {"notes": prog_items} if prog_items else None,
        })
    return blocks

def build_overall_assessment(phase1_pct:int) -> dict:
    """
    OA is based on Phase 1 only, two placeholders:
      - low range → “pleasant nudge” version
      - high range → long supportive version
    """
    if phase1_pct >= 70:
        return {
            "summary": (
              "The impact of the incident appears to be pulling you toward a state of non-adjustment. "
              "That doesn’t mean you’re failing—it means your system is still protecting you by staying close to what’s familiar. "
              "What helps now is a gentle, structured path back to safety and agency."
            ),
            "bullets": [
              "Name what hurts (briefly, once); then name one thing that matters today.",
              "Pick one tiny, repeatable action (2–5 minutes) that supports stability.",
              "Protect sleep and basic routines first; add social contact second.",
              "Schedule one supportive conversation this week (mentor, counsellor, trusted adult).",
            ],
            "key_need": "Warm structure—small steps you can repeat even on difficult days",
        }
    else:
        return {
            "summary": (
              "You’re absorbing the impact and still finding moments of steadiness—good. "
              "Keep leaning into the routines and supports that are already working."
            ),
            "bullets": [
              "Keep the helpful habits visible (checklist on phone/desk).",
              "Add one small restorative activity (walk, stretch, journaling).",
              "Notice and note progress weekly—what felt 1% easier?",
            ],
            "key_need": "Consistency—keep the small stabilisers you can control",
        }
