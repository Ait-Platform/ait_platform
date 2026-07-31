from flask import flash, redirect, render_template, request, url_for
from app.models.auth import AuthSubject
from app.models.auth import DirectMessage
from app.extensions import db
from app.admin import admin_bp




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


