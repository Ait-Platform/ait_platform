import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

bad_snippet = '''@sace_bp.route("/sace/reading/presentation")
@login_required

@sace_bp.route("/sace/acknowledge_patent", methods=["POST"])
@login_required
def acknowledge_patent():'''

good_snippet = '''@sace_bp.route("/sace/acknowledge_patent", methods=["POST"])
@login_required
def acknowledge_patent():'''

text = text.replace(bad_snippet, good_snippet)

# Let's also check for any empty presentation_complete routes. Wait, presentation_complete is missing!
# Let me verify if presentation_complete exists.
