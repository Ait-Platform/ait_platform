import re

with open('app/payments/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

fork_code = """
@quote_bp.route("/pricing/franchise-fork/<subject>")
def franchise_fork(subject):
    from app.models.auth import AuthSubject
    subj_obj = AuthSubject.query.filter_by(slug=subject).first()
    subject_name = subj_obj.name if subj_obj else subject.replace('_', ' ').title()
    return render_template("payments/franchise_fork.html", subject=subject, subject_name=subject_name)
"""

if "def franchise_fork" not in text:
    text = text + "\n" + fork_code
    with open('app/payments/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Added franchise fork route")
