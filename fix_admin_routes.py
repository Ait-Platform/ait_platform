with open('app/admin/routes.py', 'a', encoding='utf-8') as f:
    f.write('''
@admin_bp.route("/settings", methods=["GET", "POST"])
def global_settings():
    if request.method == "POST":
        quote_cents = request.form.get("mechanic_quote_cents")
        invoice_cents = request.form.get("mechanic_invoice_cents")
        enquiry_cents = request.form.get("practice_enquiry_cents")
        
        hds_cents = request.form.get("hds_subscription_cents")
        adv_reg_cents = request.form.get("adv_math_registration_cents")
        adv_sub_cents = request.form.get("adv_math_subtopic_cents")
        
        updates = []
        if quote_cents: updates.append(('mechanic_quote_cents', quote_cents))
        if invoice_cents: updates.append(('mechanic_invoice_cents', invoice_cents))
        if enquiry_cents: updates.append(('practice_enquiry_cents', enquiry_cents))
        if hds_cents: updates.append(('hds_subscription_cents', hds_cents))
        if adv_reg_cents: updates.append(('adv_math_registration_cents', adv_reg_cents))
        if adv_sub_cents: updates.append(('adv_math_subtopic_cents', adv_sub_cents))
        
        for key, val in updates:
            db.session.execute(text("INSERT INTO system_settings (key, value) VALUES (:k, :v) ON CONFLICT(key) DO UPDATE SET value=excluded.value"), {"k": key, "v": val})
            
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
''')
