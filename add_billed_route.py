import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

new_route = '''
@mechanic_bp.route("/mechanic/job_card/<int:id>/bill", methods=["POST"])
@login_required
def mark_billed(id):
    from app.models.mechanic import MechJobCard
    job_card = MechJobCard.query.get_or_404(id)
    if job_card.status in ['Approved', 'Awaiting Deposit']:
        job_card.status = 'Billed'
        db.session.commit()
        flash("Job Card marked as Completed / Billed!", "success")
    else:
        flash("Only approved jobs can be billed.", "warning")
    return redirect(request.referrer or url_for('mechanic_bp.job_card_detail', id=id))

'''

# Inject it before the def accept_quote route
content = content.replace('@mechanic_bp.route("/mechanic/job_card/<int:id>/accept", methods=["POST"])', new_route + '\n@mechanic_bp.route("/mechanic/job_card/<int:id>/accept", methods=["POST"])')

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
