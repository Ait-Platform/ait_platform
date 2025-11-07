# app/subject/loss/charts.py
from __future__ import annotations
import io, base64
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def phase_scores_bar(scores, thresholds=None, width=800, height=380):
    """
    scores: list of 4 ints (0..100) for phases 1..4
    thresholds: {"low": 33, "mid": 66, "high": 85} by default
    returns (data_uri, png_bytes)
    """
    thresholds = thresholds or {"low": 33, "mid": 66, "high": 85}
    scores = [(int(s) if s is not None else 0) for s in scores]

    fig, ax = plt.subplots(figsize=(width/100.0, height/100.0), dpi=100)

    # Background bands (light tints) to visually show low/mid/high zones
    low, mid, high = thresholds["low"], thresholds["mid"], thresholds["high"]
    ax.axhspan(0, low,  facecolor="#e5e7eb", alpha=0.5)   # light gray
    ax.axhspan(low, mid, facecolor="#dbeafe", alpha=0.5)  # light blue
    ax.axhspan(mid, 100, facecolor="#ffe4e6", alpha=0.5)  # light rose

    x = [1, 2, 3, 4]
    bars = ax.bar(x, scores, width=0.6, color=["#2563eb","#059669","#d97706","#dc2626"], edgecolor="#111827")

    # Annotate each bar with its % just above the top
    for i, (rect, y) in enumerate(zip(bars, scores), start=1):
        ax.text(i, max(y + 2, 2), f"{y}%", ha="center", va="bottom", fontsize=9)

    # Reference lines + labels on the right
    for label, y in thresholds.items():
        ax.axhline(y, linestyle="--", linewidth=1, color="#1f2937")
        ax.text(x[-1] + 0.2, y, f"{label.title()} ({y}%)", va="center", fontsize=9, color="#374151")

    ax.set_ylim(0, 100)
    ax.set_xlim(0.4, 4.8)
    ax.set_xticks(x)
    ax.set_xticklabels([str(i) for i in x])
    ax.set_xlabel("Phases")
    ax.set_ylabel("%")

    # We already have an H3 in HTML, so keep the plot itself untitled
    ax.set_title("")

    # Cleaner frame
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle=":", linewidth=0.7, alpha=0.6)

    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)

    png = buf.getvalue()
    data_uri = "data:image/png;base64," + base64.b64encode(png).decode("ascii")
    return data_uri, png


def phase_scores_bar(scores, thresholds=None, width=800, height=380):
    """
    Accepts scores as:
      - [22, 35, 18, 44]
      - {'p1':22,'p2':35,'p3':18,'p4':44}  (or 'p1_score', etc.)
      - [('p1',22),('p2',35), ...] or [(1,22), (2,35), ...]
    Normalizes to four integers (0..100) and draws the chart.
    Returns (data_uri, png_bytes)
    """
    thresholds = thresholds or {"low": 33, "mid": 66, "high": 85}

    # ---- normalize input to 4 ints
    def to_int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            try:
                return int(float(v))
            except Exception:
                return None

    norm = []

    if isinstance(scores, dict):
        # Try both key styles
        for k in ("p1", "p2", "p3", "p4", "p1_score", "p2_score", "p3_score", "p4_score"):
            if k in scores:
                v = to_int(scores.get(k))
                if v is not None:
                    norm.append(v)
    else:
        for item in (scores or []):
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                v = to_int(item[1])  # use the value part
            else:
                v = to_int(item)
            if v is not None:
                norm.append(v)

    # pad/trim to exactly 4 values
    scores = (norm + [0, 0, 0, 0])[:4]

    # ---- plotting (unchanged from your version)
    fig, ax = plt.subplots(figsize=(width/100.0, height/100.0), dpi=100)

    low, mid, high = thresholds["low"], thresholds["mid"], thresholds["high"]
    ax.axhspan(0, low,  facecolor="#e5e7eb", alpha=0.5)
    ax.axhspan(low, mid, facecolor="#dbeafe", alpha=0.5)
    ax.axhspan(mid, 100, facecolor="#ffe4e6", alpha=0.5)

    x = [1, 2, 3, 4]
    bars = ax.bar(x, scores, width=0.6,
                  color=["#2563eb", "#059669", "#d97706", "#dc2626"],
                  edgecolor="#111827")

    for i, (rect, y) in enumerate(zip(bars, scores), start=1):
        ax.text(i, max(y + 2, 2), f"{y}%", ha="center", va="bottom", fontsize=9)

    for label, y in thresholds.items():
        ax.axhline(y, linestyle="--", linewidth=1, color="#1f2937")
        ax.text(x[-1] + 0.2, y, f"{label.title()} ({y}%)", va="center",
                fontsize=9, color="#374151")

    ax.set_ylim(0, 100)
    ax.set_xlim(0.4, 4.8)
    ax.set_xticks(x)
    ax.set_xticklabels([str(i) for i in x])
    ax.set_xlabel("Phases")
    ax.set_ylabel("%")
    ax.set_title("")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle=":", linewidth=0.7, alpha=0.6)

    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)

    png = buf.getvalue()
    data_uri = "data:image/png;base64," + base64.b64encode(png).decode("ascii")
    return data_uri, png
