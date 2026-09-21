with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_nuke = """        num_deleted = db.session.query(UipResolutionVote).delete()
        db.session.commit()
        flash(f"Successfully wiped {num_deleted} test votes from the database! All tallies are now 0.", "success")"""

new_nuke = """        num_deleted = db.session.query(UipResolutionVote).delete()
        
        # Reset all resolutions to PROPOSED so they can be voted on again
        from app.models.uip import UipResolution
        resolutions = UipResolution.query.all()
        for r in resolutions:
            r.status = "PROPOSED"
            
        db.session.commit()
        flash(f"Successfully wiped {num_deleted} test votes and reset all resolutions to PROPOSED! All tallies are now 0.", "success")"""

text = text.replace(old_nuke, new_nuke)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated nuke-test-votes to also reset resolution statuses")
