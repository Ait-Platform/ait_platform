import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

simulator_route = """
@sace_bp.route("/sace/reading/simulator")
@login_required
def simulator():
    from app.models.sace import SaceDocument
    docs = SaceDocument.query.filter_by(slug='reading').all()
    doc_dict = {d.document_type: d for d in docs}
    
    return render_template("program_sace/simulator.html", docs=doc_dict)
"""

if "def simulator():" not in text:
    text += simulator_route
    with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Added simulator route")
