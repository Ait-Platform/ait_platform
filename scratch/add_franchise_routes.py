import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

franchise_routes = """
@auth_bp.route("/franchise/dashboard")
@login_required
def franchise_dashboard():
    from app.models.auth import FranchiseLicense
    # Get all licenses for this facilitator
    licenses = FranchiseLicense.query.filter_by(user_id=current_user.id).all()
    return render_template("auth/franchise_dashboard.html", licenses=licenses)

@auth_bp.route("/franchise/register_students", methods=["POST"])
@login_required
def franchise_register_students():
    from app.models.auth import FranchiseLicense, User, UserEnrollment
    from werkzeug.security import generate_password_hash
    
    license_id = request.form.get("license_id")
    emails_raw = request.form.get("emails")
    
    franchise = FranchiseLicense.query.filter_by(id=license_id, user_id=current_user.id).first()
    if not franchise:
        flash("Invalid license.", "danger")
        return redirect(url_for("auth_bp.franchise_dashboard"))
        
    # parse emails
    import re
    email_list = [e.strip().lower() for e in re.split(r'[,;\n\s]+', emails_raw) if e.strip()]
    email_list = list(set(email_list)) # remove duplicates
    
    if not email_list:
        flash("No valid emails provided.", "warning")
        return redirect(url_for("auth_bp.franchise_dashboard"))
        
    remaining = franchise.total_seats - franchise.used_seats
    if len(email_list) > remaining:
        flash(f"You only have {remaining} seats left, but you tried to add {len(email_list)} students. Please reduce the list.", "danger")
        return redirect(url_for("auth_bp.franchise_dashboard"))
        
    registered_count = 0
    for email in email_list:
        # Check if user exists
        student = User.query.filter(db.func.lower(User.email) == email).first()
        if not student:
            # Create the student account with a default password
            # In a production environment, this would trigger a password reset email
            default_pw = generate_password_hash("Student2026!")
            student = User(email=email, name=email.split('@')[0], password_hash=default_pw, is_active=1)
            db.session.add(student)
            db.session.flush() # get ID
            
        # Check if they are already enrolled
        existing_enrollment = UserEnrollment.query.filter_by(user_id=student.id, subject_id=franchise.subject_id).first()
        if not existing_enrollment:
            enrollment = UserEnrollment(user_id=student.id, subject_id=franchise.subject_id, status='active')
            db.session.add(enrollment)
            franchise.used_seats += 1
            registered_count += 1
            
    db.session.commit()
    flash(f"Successfully registered and granted access to {registered_count} students!", "success")
    return redirect(url_for("auth_bp.franchise_dashboard"))
"""

if "def franchise_dashboard" not in text:
    text += "\n" + franchise_routes
    with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Added franchise routes")
else:
    print("Franchise routes already exist")
