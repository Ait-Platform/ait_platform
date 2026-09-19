with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_post = """        elif action == "upload_photo":
            member_id = request.form.get("member_id")"""

new_post = """        elif action == "assign_member":
            seat_title = request.form.get("seat_title")
            member_id = request.form.get("member_id", type=int)
            if seat_title and member_id:
                from app.models.uip_governance import UipCommitteeMember
                member = UipCommitteeMember.query.filter_by(id=member_id, organization_id=org.id).first()
                if member:
                    member.position = seat_title
                    db.session.commit()
                    flash(f"{member.name} assigned to {seat_title}.", "success")
        elif action == "upload_photo":
            member_id = request.form.get("member_id")"""

if old_post in text:
    text = text.replace(old_post, new_post)
    print("Added assign_member backend logic")
else:
    print("Could not find POST logic")

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
