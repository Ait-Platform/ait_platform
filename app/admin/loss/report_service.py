# app/admin/loss/report_service.py
from __future__ import annotations
from typing import Any, Dict, List
from sqlalchemy import text
from app.extensions import db

def _pct(val, maxv) -> float:
    try:
        v = float(val or 0); m = float(maxv or 0)
        return 0.0 if m <= 0 else round(100.0 * v / m, 1)
    except Exception:
        return 0.0

def _band(p: float) -> int:
    return 3 if p >= 70 else (2 if p >= 40 else 1)

def _band_name(b: int) -> str:
    return "low" if b == 1 else ("medium" if b == 2 else "high")

def fetch_result_row(run_id: int) -> Dict[str, Any] | None:
    return db.session.execute(
        text("""
            SELECT id, user_id, phase_1, phase_2, phase_3, phase_4,
                   max_phase_1, max_phase_2, max_phase_3, max_phase_4,
                   total, max_total, subject, created_at
            FROM lca_result
            WHERE run_id = :rid
            ORDER BY id DESC
            LIMIT 1
        """),
        {"rid": run_id},
    ).mappings().first()

def build_phase_blocks(row: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    if not row:
        return []
    blocks: List[Dict[str, Any]] = []
    max_total = float(row.get("max_total") or 0)
    # if per-phase max is absent, fall back to max_total/4
    for i in (1, 2, 3, 4):
        raw = row.get(f"phase_{i}")
        mx  = row.get(f"max_phase_{i}") or (max_total/4.0 if max_total > 0 else 1.0)
        pct = _pct(raw, mx)
        blocks.append({
            "phase": i,
            "pct": pct,
            "width_pct": pct,
            "band": _band(pct),
            "heading": None,
            "body": None,
            # optional defaults until you have a comments source:
            "comments_list": [],
            "progress": None,
        })
    return blocks

def build_overall_assessment(blocks: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    if not blocks:
        return None
    # OA driven by Phase 1 band/label (matches your visual rules)
    p1 = next((b for b in blocks if b["phase"] == 1), None)
    if not p1:
        return None
    p1_band = p1["band"]
    oa_label = "Not Coping" if p1_band == 3 else ("Slightly Coping" if p1_band == 2 else "Coping")
    band_name = _band_name(p1_band)

    row = db.session.execute(
        text("""
            SELECT label, key_need, body, tone
            FROM lca_overall_item
            WHERE active = 1 AND type = 'summary' AND band = :band
            ORDER BY ordinal ASC
            LIMIT 1
        """),
        {"band": band_name},
    ).mappings().first()

    summary_txt = (row.get("body") if row else "") or ""
    parts = [p.strip() for p in summary_txt.splitlines() if p.strip()]
    oa_summary = parts[0] if parts else ""
    oa_bullets = [p for p in parts[1:] if len(p) <= 140]

    return {
        "label": oa_label,
        "summary": oa_summary,
        "bullets": oa_bullets,
        "key_need": (row.get("key_need") if row else None),
    }
