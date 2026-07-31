# app/admin/loss/progress.py
from .utils import compute_adaptive_vector

def build_loss_progress_context(phase_percentages: dict) -> dict:
    """
    Returns the extra context the report needs.
    Keep this small: only Adaptive Vector for now.
    """
    return {
        "adaptive_vector": compute_adaptive_vector(phase_percentages)
    }
