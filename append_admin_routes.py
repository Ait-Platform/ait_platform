
@admin_bp.route("/settings", methods=["GET", "POST"])
def global_settings():
    if request.method == "POST":
        quote_cents = request.form.get("mechanic_quote_cents")
        invoice_cents = request.form.get("mechanic_invoice_cents")
        enquiry_cents = request.form.get("practice_enquiry_cents")
        
        if quote_cents and invoice_cents:
            db.session.execute(text("UPDATE system_settings SET value = :val, updated_at = CURRENT_TIMESTAMP WHERE key = 'mechanic_quote_cents'"), {"val": quote_cents})
            db.session.execute(text("UPDATE system_settings SET value = :val, updated_at = CURRENT_TIMESTAMP WHERE key = 'mechanic_invoice_cents'"), {"val": invoice_cents})
        
        if enquiry_cents:
            # We use an UPDATE here because we already INSERTED it in the script.
            db.session.execute(text("UPDATE system_settings SET value = :val, updated_at = CURRENT_TIMESTAMP WHERE key = 'practice_enquiry_cents'"), {"val": enquiry_cents})
            
        db.session.commit()
        flash("Global settings updated successfully", "success")
        return redirect(url_for("admin_bp.global_settings"))
        
    settings = db.session.execute(text("SELECT key, value FROM system_settings")).fetchall()
    settings_dict = {s.key: s.value for s in settings}
    return render_template("admin/settings.html", settings=settings_dict)

from app.models.auth import DirectMessage
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

# Will append to admin routes

