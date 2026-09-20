with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """        elif action == "upload_photo":
            member_id = request.form.get("member_id")
            photo_url = request.form.get("photo_url")
            member = UipCommitteeMember.query.get(member_id)
            if member and member.organization_id == org.id:
                member.photo_url = photo_url
                db.session.commit()
                flash(f"Photo uploaded for {member.name}.", "success")"""

new_logic = """        elif action == "upload_photo":
            member_id = request.form.get("member_id")
            photo_file = request.files.get("photo_file")
            member = UipCommitteeMember.query.get(member_id)
            if member and member.organization_id == org.id and photo_file:
                try:
                    from app.utils.cloudflare_r2 import upload_file_to_r2
                    photo_url = upload_file_to_r2(photo_file)
                    member.photo_url = photo_url
                    db.session.commit()
                    flash(f"Photo uploaded for {member.name} to Cloudflare R2.", "success")
                except Exception as e:
                    flash(f"Failed to upload photo: {str(e)}", "danger")"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated upload_photo handler")
