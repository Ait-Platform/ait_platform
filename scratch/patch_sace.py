import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_route = """@sace_bp.route("/sace/compliance/annexure_a")
@login_required
def annexure_a():
    return render_template("program_sace/compliance/annexure_a.html")"""

new_route = """@sace_bp.route("/sace/compliance/annexure_a")
@login_required
def annexure_a():
    from app.models.sace import SaceDocument
    tt_doc = SaceDocument.query.filter_by(slug='reading', document_type='timetable').first()
    return render_template("program_sace/compliance/annexure_a.html", tt_doc=tt_doc)"""

text = text.replace(old_route, new_route)

# And fix the 500 error in evaluator_report
text = text.replace("order_by(SaceWorkshopInteraction.created_at)", "order_by(SaceWorkshopInteraction.timestamp)")

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
