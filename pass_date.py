import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

job_card_detail_original = '''@mechanic_bp.route("/mechanic/job/<int:id>", methods=["GET", "POST"])
@login_required
def job_card_detail(id):
    job_card = MechJobCard.query.get_or_404(id)
    return render_template("program_mechanic/job_card.html", job_card=job_card)'''

job_card_detail_new = '''@mechanic_bp.route("/mechanic/job/<int:id>", methods=["GET", "POST"])
@login_required
def job_card_detail(id):
    from datetime import datetime
    job_card = MechJobCard.query.get_or_404(id)
    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    return render_template("program_mechanic/job_card.html", job_card=job_card, today_date=today_date)'''

content = content.replace(job_card_detail_original, job_card_detail_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated job_card_detail to pass today_date")
