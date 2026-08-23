import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_commit = '''        ilog = InviteLog(
            sender_id=current_user.id,
            recipient_phone=phone,
            program_slug="mechanic",
            invite_type=f"Created Job Card #{job_card.job_number}",
            status="Logged"
        )
        db.session.add(ilog)
                
        db.session.commit()
            
        flash("Quote created and Job Card generated successfully!", "success")
        return redirect(url_for("mechanic_bp.job_card_detail", id=job_card.id))'''

new_commit = '''        ilog = InviteLog(
            sender_id=current_user.id,
            recipient_phone=phone,
            program_slug="mechanic",
            invite_type=f"Created Job Card #{job_card.job_number}",
            status="Logged"
        )
        db.session.add(ilog)
                
        try:
            db.session.commit()
            flash("Quote created and Job Card generated successfully!", "success")
            return redirect(url_for("mechanic_bp.job_card_detail", id=job_card.id))
        except Exception as e:
            db.session.rollback()
            flash(f"An error occurred while saving the quote: {str(e).split('DETAIL:')[0].strip()}", "danger")
            return redirect(url_for('mechanic_bp.new_quote'))'''

content = content.replace(old_commit, new_commit)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
