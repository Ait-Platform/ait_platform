# app/schools/loss/views.py
# app/admin/loss/views.py
from flask import render_template, abort
from app.admin import admin_bp
from .utils import build_loss_result_summary
'''
@admin_bp.route("/loss/result/<int:run_id>")
def loss_result(run_id: int):
    phase_percentages = _get_phase_percentages_for_run(run_id)
    if phase_percentages is None:
        abort(404)
    adaptive_vector, phases = build_loss_result_summary(phase_percentages)
    return render_template(
        "admin/loss/result.html",
        run_id=run_id,
        adaptive_vector=adaptive_vector,
        phases=phases,
    )
'''
@admin_bp.route("/loss/sequence/<int:pos>")
def loss_sequence_step(pos: int):
    # keep your existing sequence logic here
    # ...
    return render_template("admin/loss/sequence_step.html", pos=pos)

def _get_phase_percentages_for_run(run_id: int):
    # Replace with real DB aggregation; demo allows you to see UI immediately.
    return {1: 33.0, 2: 48.0, 3: 72.0, 4: 0.0, 5: 88.0}
