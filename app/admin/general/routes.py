from flask import flash, redirect, render_template, request, url_for
from app.models.auth import AuthSubject
from app.models.auth import DirectMessage
from app.extensions import db
from app.admin import admin_bp

@admin_bp.route("/programs", methods=["GET", "POST"])
def manage_programs():
    from app.models.auth import AuthSubject
    if request.method == "POST":
        subject_id = request.form.get("subject_id")
        is_hidden = request.form.get("is_hidden") == "1"
        req_price = request.form.get("requires_price") == "1"
        ptype = request.form.get("program_type")
        subj = AuthSubject.query.get(subject_id)
        if subj:
            subj.is_hidden_on_bridge = is_hidden
            subj.requires_price = req_price
            subj.program_type = ptype
            db.session.commit()
            flash(f"Updated {subj.name}", "success")
        return redirect(url_for("admin_bp.manage_programs"))
    
    subjects = AuthSubject.query.order_by(AuthSubject.name).all()
    return render_template("admin/programs.html", subjects=subjects)


@admin_bp.route('/messages', methods=['GET', 'POST'])
def view_messages():
    if request.method == 'POST':
        msg_id = request.form.get('msg_id')
        reply = request.form.get('reply')
        msg = DirectMessage.query.get(msg_id)
        if msg and reply:
            msg.reply = reply
            msg.is_read = True
            db.session.commit()
            flash('Reply sent successfully', 'success')
        return redirect(url_for('admin_bp.view_messages'))
        
    msgs = DirectMessage.query.order_by(DirectMessage.created_at.desc()).all()
    return render_template('admin/messages.html', messages=msgs)


