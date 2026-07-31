# app/admin/loss/utils.py
from dataclasses import dataclass
from typing import Dict, List, Tuple
from .progress_texts import CATALOG
from flask import request, session, abort

@dataclass
class ProgressItem:
    idx: int
    label: str
    text: str
    sentiment: str  # 'positive' | 'slightly' | 'negative'

@dataclass
class PhaseReport:
    name: str
    percent: int
    band: str               # 'low'|'mid'|'high'
    items: List[ProgressItem]
    headline: str

def _band_for(pct: int) -> str:
    if pct >= 70: return "high"
    if pct >= 40: return "mid"
    return "low"

def _headline_for(band: str) -> str:
    return "Positive" if band == "high" else ("Slightly Positive" if band == "mid" else "Negative")

def _sentiment_for(band: str) -> str:
    return "positive" if band == "high" else ("slightly" if band == "mid" else "negative")

def adaptive_vector_label(pcts: List[int]) -> str:
    if not pcts: return "Not Coping"
    avg = round(sum(pcts)/len(pcts))
    return "Coping" if avg >= 70 else ("Slightly Coping" if avg >= 40 else "Not Coping")

def _phase_items(phase: int, band: str) -> List[ProgressItem]:
    cfg = CATALOG.get(phase, [])
    out: List[ProgressItem] = []
    for i, item in enumerate(cfg, start=1):
        text = item.get(band) or ""    # safe if any missing copy
        out.append(ProgressItem(
            idx=i,
            label=item.get("label", f"Item {i}"),
            text=text,
            sentiment=_sentiment_for(band),
        ))
    # always return exactly 3 items (pad if catalog shorter)
    while len(out) < 3:
        k = len(out) + 1
        out.append(ProgressItem(idx=k, label=f"Item {k}", text="", sentiment=_sentiment_for(band)))
    return out

def build_loss_progress_analysis(phase_percentages: Dict[int, float]) -> Tuple[str, List[PhaseReport], Dict[str, List[tuple]]]:
    """
    Returns:
      adaptive_vector (str),
      phases: [PhaseReport...],
      summary: {
        'positives': [(phase_name, item_label, text)],
        'negatives': [(phase_name, item_label, text)],
        'slightly':  [(phase_name, item_label, text)],
      }
    """
    phases: List[PhaseReport] = []
    for idx in sorted(phase_percentages.keys()):
        pct = int(round(phase_percentages[idx] or 0))
        band = _band_for(pct)
        items = _phase_items(idx, band)
        phases.append(PhaseReport(
            name=f"Phase {idx}",
            percent=pct,
            band=band,
            items=items,
            headline=_headline_for(band),
        ))

    vector = adaptive_vector_label([p.percent for p in phases])

    positives, negatives, slightly = [], [], []
    for p in phases:
        for it in p.items:
            tup = (p.name, it.label, it.text)
            if it.sentiment == "positive": positives.append(tup)
            elif it.sentiment == "slightly": slightly.append(tup)
            else: negatives.append(tup)

    summary = {
        "positives": positives,
        "slightly": slightly,
        "negatives": negatives,
    }
    return vector, phases, summary
# Phase polarity: -1 = inverse (lower is better), +1 = direct (higher is better)
PHASE_POLARITY: dict[int, int] = {
    1: -1,  # lower is better
    2: -1,  # lower is better
    3: -1,  # lower is better
    4: +1,  # higher is better
    5: +1,  # higher is better
}

def compute_adaptive_vector(phase_percentages: dict) -> str:
    """
    Extreme/middle rule based only on phases 1–3 (phase 4 ignored):
      - all three <= 50%  -> "Coping"
      - all three >  50%  -> "Not Coping"
      - otherwise         -> "Slightly Coping"
    Keys may be "1","2","3" or ints; we coerce safely.
    """
    if not phase_percentages:
        return "Slightly Coping"

    def _raw(ph: int):
        # get raw % for a phase; None if missing
        for k in (ph, str(ph)):
            if k in phase_percentages:
                try:
                    return int(round(phase_percentages[k] or 0))
                except Exception:
                    return 0
        return None

    p1, p2, p3 = _raw(1), _raw(2), _raw(3)

    # If any of phases 1–3 are missing, treat as middle ground
    if p1 is None or p2 is None or p3 is None:
        return "Slightly Coping"

    le50 = sum(1 for v in (p1, p2, p3) if v <= 50)
    gt50 = sum(1 for v in (p1, p2, p3) if v > 50)

    if le50 == 3:
        return "Coping"
    if gt50 == 3:
        return "Not Coping"
    return "Slightly Coping"

# app/admin/loss/utils.py


def get_run_id(required: bool = True) -> int | None:
    rid = None
    if request.method in ("POST","PUT","PATCH"):
        rid = request.form.get("run_id", type=int)
        if rid is None:
            data = request.get_json(silent=True) or {}
            val = data.get("run_id")
            try:
                rid = int(val) if val is not None else None
            except Exception:
                rid = None
    if rid is None:
        rid = request.args.get("run_id", type=int)
    if rid is None:
        rid = session.get("current_run_id") or session.get("last_loss_run_id")
    if required and not rid:
        abort(400, description="run_id is required")
    return rid

def with_run_id_in_ctx(ctx: dict | None = None, run_id: int | None = None) -> dict:
    ctx = dict(ctx or {})
    if run_id is None:
        run_id = get_run_id(required=True)
    ctx.pop("run_id", None)
    ctx["run_id"] = run_id
    return ctx

def get_run_id(default=None):
    return (
        request.args.get("run_id", type=int)
        or request.args.get("rid", type=int)   # old links still work
        or session.get("current_run_id")
        or default
    )
