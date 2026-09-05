import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_route = """@sace_bp.route("/sace/reading/simulator")
@login_required
def simulator():
    from app.models.sace import SaceDocument
    docs = SaceDocument.query.filter_by(slug='reading').all()
    doc_dict = {d.document_type: d for d in docs}
    
    return render_template("program_sace/simulator.html", docs=doc_dict)"""

new_route = """from flask import make_response

@sace_bp.route("/sace/reading/simulator")
@login_required
def simulator():
    from app.models.sace import SaceDocument
    docs = SaceDocument.query.filter_by(slug='reading').all()
    doc_dict = {d.document_type: d for d in docs}
    
    response = make_response(render_template("program_sace/simulator.html", docs=doc_dict))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response"""

text = text.replace(old_route, new_route)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
