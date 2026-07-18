import re

with open('app/program_culturalfire/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_logic = '''    from app.models.culturalfire import CfiWallet
    wallet = CfiWallet.query.filter_by(user_id=current_user.id).first()
    token_balance = wallet.balance if wallet else 0

    if not paid_tokens and not active_assignments:
        return render_template("program_culturefire/judge_dashboard.html", state="purchase", subject=judge_subject, back_origin=back_origin, back_enrollment_id=back_enrollment_id, token_balance=token_balance)'''
text = re.sub(r'    if not paid_tokens and not active_assignments:\n\s*return render_template\("program_culturefire/judge_dashboard.html", state="purchase"[^\n]*', new_logic, text)

new_logic2 = '''    if active_assignments:
        return render_template("program_culturefire/judge_dashboard.html", state="assigned", assignments=active_assignments, back_origin=back_origin, back_enrollment_id=back_enrollment_id, token_balance=token_balance)'''
text = re.sub(r'    if active_assignments:\n\s*return render_template\("program_culturefire/judge_dashboard.html", state="assigned"[^\n]*', new_logic2, text)

new_logic3 = '''    return render_template("program_culturefire/judge_dashboard.html", 
                           state="selection", 
                           available_shows=available_shows, 
                           tokens=len(paid_tokens),
                           back_origin=back_origin,
                           back_enrollment_id=back_enrollment_id,
                           token_balance=token_balance)'''
text = re.sub(r'    return render_template\("program_culturefire/judge_dashboard.html", \n\s*state="selection", \n\s*available_shows=available_shows, \n\s*tokens=len\(paid_tokens\),\n\s*back_origin=back_origin,\n\s*back_enrollment_id=back_enrollment_id\)', new_logic3, text)

new_route = '''@cultural_bp.route("/judge/purchase_tokens", methods=["POST"])
@login_required
def purchase_judge_with_tokens():
    from app.models.auth import AuthSubject, UserEnrollment
    from app.program_culturalfire.helpers import charge_tokens
    
    if not charge_tokens(current_user.id, 10, "Judge Application"):
        flash("Insufficient tokens to purchase judge application. Please top up your wallet.", "warning")
        return redirect(url_for("cultural_bp.wallet_dashboard"))
        
    judge_subject = AuthSubject.query.filter_by(slug='cfi_judge').first()
    if not judge_subject:
        flash("Judge application system is currently unavailable.", "error")
        return redirect(url_for('cultural_bp.judge_dashboard'))
        
    enrollment = UserEnrollment(
        user_id=current_user.id,
        subject_id=judge_subject.id,
        status='paid'
    )
    from app.extensions import db
    db.session.add(enrollment)
    db.session.commit()
    
    flash("Successfully purchased Judge Application! You can now select a show.", "success")
    return redirect(url_for('cultural_bp.judge_dashboard'))

@cultural_bp.route("/judge/dashboard")'''
text = text.replace('@cultural_bp.route("/judge/dashboard")', new_route)

with open('app/program_culturalfire/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("Updated routes.py successfully.")
