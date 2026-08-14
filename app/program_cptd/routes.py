from flask import render_template, request, redirect, url_for, flash, current_app
from flask_login import login_required, current_user
from datetime import datetime
from app.extensions import db
from app.models.cptd import CptdRegistration, CptdProgress, CptdEvaluation
from . import cptd_bp

@cptd_bp.before_request
def check_hard_close():
    # If it's past 15:00 on the workshop date, prevent any state changes
    # We will refine this later if needed
    pass

@cptd_bp.route("/cptd/hub")
@login_required
def hub():
    # Show the 4 programmes: Reading, Cultural Fire, LOLO, HOME
    registrations = CptdRegistration.query.filter_by(user_id=current_user.id).all()
    reg_dict = {reg.programme: reg for reg in registrations}
    return render_template("program_cptd/hub.html", registrations=reg_dict)

@cptd_bp.route("/cptd/register/<programme>", methods=["POST"])
@login_required
def register(programme):
    if programme != 'reading':
        flash("This programme is currently locked.", "warning")
        return redirect(url_for('cptd_bp.hub'))
        
    reg = CptdRegistration.query.filter_by(user_id=current_user.id, programme=programme).first()
    if not reg:
        reg = CptdRegistration(
            user_id=current_user.id,
            programme=programme,
            workshop_date=datetime.utcnow().date(),
            status='registered'
        )
        db.session.add(reg)
        
        # Initialize timetable progress items
        modules = ['1', '2', '3', '4', 'eval']
        for mod in modules:
            status = 'unlocked' if mod == '1' else 'locked'
            progress = CptdProgress(
                user_id=current_user.id,
                programme=programme,
                module_id=mod,
                status=status
            )
            db.session.add(progress)
            
        db.session.commit()
        flash("Successfully registered for the CPTD Workshop!", "success")
        
    return redirect(url_for('cptd_bp.reading_timetable'))

@cptd_bp.route("/cptd/reading/timetable")
@login_required
def reading_timetable():
    reg = CptdRegistration.query.filter_by(user_id=current_user.id, programme='reading').first()
    if not reg:
        flash("Please register for the workshop first.", "warning")
        return redirect(url_for('cptd_bp.hub'))
        
    progress_items = CptdProgress.query.filter_by(user_id=current_user.id, programme='reading').all()
    progress_dict = {p.module_id: p for p in progress_items}
    
    return render_template("program_cptd/reading_timetable.html", progress=progress_dict, reg=reg)

@cptd_bp.route("/cptd/reading/module/<module_id>")
@login_required
def reading_module(module_id):
    progress = CptdProgress.query.filter_by(user_id=current_user.id, programme='reading', module_id=module_id).first_or_404()
    if progress.status == 'locked':
        flash("This module is locked. Please complete the previous modules first.", "warning")
        return redirect(url_for('cptd_bp.reading_timetable'))
        
    return render_template(f"program_cptd/modules/reading_{module_id}.html", progress=progress)

@cptd_bp.route("/cptd/reading/checkpoint/<module_id>", methods=["POST"])
@login_required
def submit_checkpoint(module_id):
    progress = CptdProgress.query.filter_by(user_id=current_user.id, programme='reading', module_id=module_id).first_or_404()
    
    # Save evidence if any
    evidence = request.form.get("evidence", "")
    progress.evidence_data = evidence
    progress.status = 'completed'
    progress.completed_at = datetime.utcnow()
    
    # Unlock next module
    next_map = {'1': '2', '2': '3', '3': '4', '4': 'eval'}
    if module_id in next_map:
        next_mod = CptdProgress.query.filter_by(user_id=current_user.id, programme='reading', module_id=next_map[module_id]).first()
        if next_mod and next_mod.status == 'locked':
            next_mod.status = 'unlocked'
            
    db.session.commit()
    flash("Checkpoint completed successfully!", "success")
    return redirect(url_for('cptd_bp.reading_timetable'))

@cptd_bp.route("/cptd/reading/evaluate", methods=["GET", "POST"])
@login_required
def reading_evaluate():
    progress = CptdProgress.query.filter_by(user_id=current_user.id, programme='reading', module_id='eval').first_or_404()
    if progress.status == 'locked':
        flash("Please complete all modules before evaluating.", "warning")
        return redirect(url_for('cptd_bp.reading_timetable'))
        
    if request.method == "POST":
        eval_record = CptdEvaluation(
            user_id=current_user.id,
            programme='reading',
            rating_programme=int(request.form.get('rating_programme', 0)),
            rating_facilitator=int(request.form.get('rating_facilitator', 0)),
            rating_platform=int(request.form.get('rating_platform', 0)),
            feedback_text=request.form.get('feedback_text', '')
        )
        db.session.add(eval_record)
        
        progress.status = 'completed'
        progress.completed_at = datetime.utcnow()
        
        reg = CptdRegistration.query.filter_by(user_id=current_user.id, programme='reading').first()
        if reg:
            reg.status = 'completed'
            
        db.session.commit()
        flash("Thank you! Your evaluation has been submitted and the workshop is complete.", "success")
        return redirect(url_for('cptd_bp.hub'))
        
    return render_template("program_cptd/evaluation.html", progress=progress)
