with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

start_marker = "session.pop(\"just_paid_subject_id\", None)"
end_marker = "return redirect(url_for(\"auth_bp.dashboard_info\", subject=subject))"

new_injection = """
            # SACE Pre-Registered Personnel Override
            from app.models.auth import AuthSubjectAdmin, AuthSubject
            is_sace_admin = AuthSubjectAdmin.query.join(AuthSubject).filter(
                AuthSubjectAdmin.email == email_norm,
                AuthSubject.slug.like('sace_%')
            ).first()
            if is_sace_admin:
                return redirect(url_for("sace_bp.dashboard"))
                
            return redirect(url_for("auth_bp.dashboard_info", subject=subject))"""

if start_marker in text and end_marker in text:
    old_snippet = text[text.find(start_marker):text.find(end_marker) + len(end_marker)]
    new_snippet = start_marker + new_injection
    text = text.replace(old_snippet, new_snippet)
    
    with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed seamless login routing")
else:
    print("Markers not found")
