with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

route_code = '''
@budget_bp.route("/support", methods=["GET", "POST"])
@login_required
def support_request():
    if request.method == "POST":
        subject = (request.form.get("subject") or "").strip()
        message = (request.form.get("message") or "").strip()
        
        if not subject or not message:
            flash("Please provide both a subject and a message.", "warning")
            return redirect(url_for("budget_bp.support_request"))
            
        try:
            from flask_mail import Message
            from app.extensions import mail
            from flask import current_app
            
            user_info = f"User: {current_user.name} ({current_user.email})\\n\\n"
            full_message = user_info + message
            
            msg = Message(
                subject=f"[BudgetCash Support] {subject}",
                recipients=[current_app.config.get("CONTACT_TO_EMAIL", "ait@mathwithhands.com")],
                body=full_message,
                reply_to=current_user.email
            )
            mail.send(msg)
            flash("Your support request has been sent! We will respond within 24 hours.", "success")
            return redirect(url_for("budget_bp.dashboard"))
        except Exception as e:
            flash(f"Failed to send message: {str(e)}", "warning")
            return redirect(url_for("budget_bp.support_request"))
            
    return render_template("program_budget/support.html")
'''

text += route_code

with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated routes.py successfully')
