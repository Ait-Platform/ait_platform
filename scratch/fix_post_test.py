import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace duplicated decorators
bad_decorators = '''@sace_bp.route("/sace/reading/post_test", methods=["GET"])
@login_required

@sace_bp.route("/sace/reading/post_test", methods=["GET"])
@login_required
def post_test():'''

good_decorators = '''@sace_bp.route("/sace/reading/post_test", methods=["GET"])
@login_required
def post_test():'''

text = text.replace(bad_decorators, good_decorators)

# Also check for trailing whitespace or similar variations
pattern = r'@sace_bp\.route\("/sace/reading/post_test", methods=\["GET"\]\)\s*@login_required\s*@sace_bp\.route\("/sace/reading/post_test", methods=\["GET"\]\)\s*@login_required\s*def post_test\(\):'
text = re.sub(pattern, good_decorators, text)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
