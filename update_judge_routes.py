import re

with open('app/program_culturalfire/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. We will replace the entire judge_dashboard and select_show functions.
start_idx = text.find('@cultural_bp.route("/judge/purchase_tokens"')
if start_idx == -1:
    start_idx = text.find('@cultural_bp.route("/judge/dashboard")')

end_idx = text.find('@cultural_bp.route("/admin/cultural_fire")')

new_code = '''@cultural_bp.route("/judge/dashboard")
@login_required
def judge_dashboard():
    # Handle incoming origin data for dynamic back button
    origin = request.args.get('origin')
    enrollment_id = request.args.get('enrollment_id', type=int)
    
    if origin:
        session['cfi_judge_origin'] = origin
    if enrollment_id:
        session['cfi_judge_origin_enrollment'] = enrollment_id
        
    back_origin = session.get('cfi_judge_origin')
    back_enrollment_id = session.get('cfi_judge_origin_enrollment')

    from app.models.culturalfire import CfiWallet
    wallet = CfiWallet.query.filter_by(user_id=current_user.id).first()
    token_balance = wallet.balance if wallet else 0

    # Check if they are currently assigned to any active shows
    active_assignments = CfiJudgeAssignment.query.join(CfiShow).filter(
        CfiJudgeAssignment.judge_id == current_user.id,
        CfiShow.status == 'active'
    ).all()
    
    # Active shows that have slots available
    shows = CfiShow.query.filter_by(status='active').all()
    available_shows = []
    
    for show in shows:
        current_judges = CfiJudgeAssignment.query.filter_by(show_id=show.id).count()
        is_pageant = show.category_item and show.category_item.name == 'Pageant'
        max_judges = 5 if is_pageant else 3
        
        # Check if already a judge or participant
        already_judge = CfiJudgeAssignment.query.filter_by(show_id=show.id, judge_id=current_user.id).first()
        is_participant = CfiTalentSubmission.query.filter_by(show_id=show.id, user_id=current_user.id).first()
        
        if not already_judge and not is_participant and current_judges < max_judges:
            available_shows.append({
                "show": show,
                "current_judges": current_judges,
                "max_judges": max_judges
            })
            
    return render_template(
        "program_culturefire/judge_dashboard.html", 
        assignments=active_assignments,
        available_shows=available_shows,
        token_balance=token_balance,
        back_origin=back_origin,
        back_enrollment_id=back_enrollment_id
    )

@cultural_bp.route("/judge/select_show/<int:show_id>", methods=["POST"])
@login_required
def select_show(show_id):
    show = CfiShow.query.get_or_404(show_id)
    
    # Check limits
    current_judges = CfiJudgeAssignment.query.filter_by(show_id=show.id).count()
    is_pageant = show.category_item and show.category_item.name == 'Pageant'
    max_judges = 5 if is_pageant else 3
    
    if current_judges >= max_judges:
        flash("This show has reached its judge limit.", "warning")
        return redirect(url_for('cultural_bp.judge_dashboard'))
        
    already_judge = CfiJudgeAssignment.query.filter_by(show_id=show.id, judge_id=current_user.id).first()
    if already_judge:
        flash("You are already a judge for this show.", "warning")
        return redirect(url_for('cultural_bp.judge_dashboard'))

    from app.program_culturalfire.helpers import charge_tokens
    if not charge_tokens(current_user.id, 10, f"Judge Assignment: {show.title}"):
        flash("Insufficient tokens. You need 10 tokens to judge a show. Please top up your wallet.", "error")
        return redirect(url_for("cultural_bp.wallet_dashboard"))
    
    # Create Assignment
    new_assignment = CfiJudgeAssignment(
        judge_id=current_user.id,
        show_id=show.id,
        role="paid_judge"
    )
    
    db.session.add(new_assignment)
    db.session.commit()
    
    flash(f"You have been assigned as a judge for '{show.title}'! 10 tokens were deducted.", "success")
    return redirect(url_for('cultural_bp.judge_dashboard'))

'''

new_text = text[:start_idx] + new_code + text[end_idx:]

with open('app/program_culturalfire/routes.py', 'w', encoding='utf-8') as f:
    f.write(new_text)

print("Updated routes.py successfully!")
