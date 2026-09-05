import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_code = """            if is_ok:
                # Password matches, log them in seamlessly
                login_user(existing_user, fresh=True)
                flash("Welcome back! We logged you in automatically.", "success")
                
                # Replicate login session scaffolding
                session["is_authenticated"] = True
                session["email"] = email_norm
                session["user_id"] = int(existing_user.id)
                session["user_name"] = existing_user.name or email_norm.split("@")[0]
                session.pop("just_paid_subject_id", None)
                
                return redirect(url_for("auth_bp.dashboard_info", subject=subject))"""

new_code = """            if is_ok:
                # Password matches, log them in seamlessly
                login_user(existing_user, fresh=True)
                flash("Welcome back! We logged you in automatically.", "success")
                
                # Replicate login session scaffolding
                session["is_authenticated"] = True
                session["email"] = email_norm
                session["user_id"] = int(existing_user.id)
                session["user_name"] = existing_user.name or email_norm.split("@")[0]
                session.pop("just_paid_subject_id", None)
                
                # SACE Pre-Registered Personnel Override
                from app.models.auth import AuthSubjectAdmin, AuthSubject
                is_sace_admin = AuthSubjectAdmin.query.join(AuthSubject).filter(
                    AuthSubjectAdmin.email == email_norm,
                    AuthSubject.slug.like('sace_%')
                ).first()
                if is_sace_admin:
                    return redirect(url_for("sace_bp.dashboard"))
                
                return redirect(url_for("auth_bp.dashboard_info", subject=subject))"""

if old_code in text:
    text = text.replace(old_code, new_code)
    with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed seamless login routing")
else:
    print("Could not find seamless login block")
