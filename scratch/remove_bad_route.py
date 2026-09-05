import re

with open('app/payments/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

bad_code = """
@quote_bp.route("/pricing/franchise-fork/<subject>")
def franchise_fork(subject):
    from app.models.auth import AuthSubject
    subj_obj = AuthSubject.query.filter_by(slug=subject).first()
    subject_name = subj_obj.name if subj_obj else subject.replace('_', ' ').title()
    return render_template("payments/franchise_fork.html", subject=subject, subject_name=subject_name)
"""

if bad_code in text:
    text = text.replace(bad_code, "")
    with open('app/payments/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Removed from payments/routes.py")
else:
    print("Not found in payments/routes.py")
