import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_route = '''
@sace_bp.route("/sace/reading/presentation")
@login_required
def presentation():
    return render_template("program_sace/presentation_ppp.html")
'''

text = text.replace('def simulator():', new_route + '\n@sace_bp.route("/sace/reading/simulator")\n@login_required\ndef simulator():')

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
