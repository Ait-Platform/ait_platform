with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

wipe_route = """
@uip_bp.route("/<org_slug>/nuke-test-votes")
@login_required
def nuke_test_votes(org_slug):
    # TEMPORARY ROUTE FOR TESTING - WIPES ALL VOTES
    from app.models.uip import UipResolutionVote
    from app.extensions import db
    from flask import flash, redirect, url_for
    
    try:
        num_deleted = db.session.query(UipResolutionVote).delete()
        db.session.commit()
        flash(f"Successfully wiped {num_deleted} test votes from the database! All tallies are now 0.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error wiping votes: {str(e)}", "error")
        
    return redirect(url_for('uip_bp.dashboard', org_slug=org_slug))
"""

if "def nuke_test_votes" not in text:
    text = text + wipe_route

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Added nuke_test_votes route")
