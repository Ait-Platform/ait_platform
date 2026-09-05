import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_code = """            session.pop("just_paid_subject_id", None)
            
            return redirect(url_for("auth_bp.dashboard_info", subject=subject))
        else:"""

new_code = """            session.pop("just_paid_subject_id", None)
            
            # SACE Pre-Registered Personnel Override
            from app.models.auth import AuthSubjectAdmin, AuthSubject
            is_sace_admin = AuthSubjectAdmin.query.join(AuthSubject).filter(
                AuthSubjectAdmin.email == email_norm,
                AuthSubject.slug.like('sace_%')
            ).first()
            if is_sace_admin:
                return redirect(url_for("sace_bp.dashboard"))
                
            return redirect(url_for("auth_bp.dashboard_info", subject=subject))
        else:"""

if old_code in text:
    text = text.replace(old_code, new_code)
    with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Replaced successfully")
else:
    print("Not found")
