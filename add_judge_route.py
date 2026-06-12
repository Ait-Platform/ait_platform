with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

new_route = """
@cultural_bp.route("/show/<int:show_id>/apply_judge", methods=["POST"])
@login_required
def apply_judge(show_id):
    original_show = CfiShow.query.get_or_404(show_id)
    
    # 1. Check if user is a participant in this specific show
    is_participant = CfiTalentSubmission.query.filter_by(show_id=show_id, user_id=current_user.id).first()
    if is_participant:
        flash("You are a participant in this show and cannot judge it.", "warning")
        return redirect(url_for('cultural_bp.showcase_dashboard'))
        
    # 2. Check if user is already a judge for THIS show
    already_judge = CfiJudgeAssignment.query.filter_by(show_id=show_id, judge_id=current_user.id).first()
    if already_judge:
        flash("You are already assigned as a judge for this show!", "info")
        return redirect(url_for('cultural_bp.showcase_dashboard'))
        
    # 3. FCFS Logic & Rollover
    target_show = original_show
    current_judges = CfiJudgeAssignment.query.filter_by(show_id=target_show.id).count()
    
    if current_judges >= 5:
        # Rollover: Find next active show where user is NOT a participant and judges < 5
        all_active_shows = CfiShow.query.filter_by(status='active').order_by(CfiShow.start_date.asc()).all()
        found_alternative = False
        for alt_show in all_active_shows:
            # Skip the original full show
            if alt_show.id == original_show.id:
                continue
                
            # Is user participant?
            if CfiTalentSubmission.query.filter_by(show_id=alt_show.id, user_id=current_user.id).first():
                continue
                
            # Are there < 5 judges?
            alt_judges = CfiJudgeAssignment.query.filter_by(show_id=alt_show.id).count()
            if alt_judges < 5:
                # Is user already a judge for this alternative?
                if CfiJudgeAssignment.query.filter_by(show_id=alt_show.id, judge_id=current_user.id).first():
                    continue
                
                target_show = alt_show
                found_alternative = True
                break
                
        if not found_alternative:
            flash("All available shows currently have the maximum number of judges.", "warning")
            return redirect(url_for('cultural_bp.showcase_dashboard'))
            
        flash(f"'{original_show.title}' was full, so you have been automatically assigned to judge '{target_show.title}' instead!", "success")
    else:
        flash(f"You have been successfully assigned as a judge for '{target_show.title}'!", "success")

    new_assignment = CfiJudgeAssignment(
        judge_id=current_user.id,
        show_id=target_show.id,
        role="volunteer_judge"
    )
    db.session.add(new_assignment)
    db.session.commit()
    
    return redirect(url_for('cultural_bp.showcase_dashboard'))
"""

if "def apply_judge(" not in content:
    content += new_route
    with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Added apply_judge route.")
else:
    print("Route already exists.")
