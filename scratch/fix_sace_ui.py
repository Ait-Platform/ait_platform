import re

with open('app/admin/security/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_code = """                # Enroll in SACE subject to grant access
                if sace_subject:
                    enrollment = UserEnrollment(
                        user_id=user.id,
                        subject_id=sace_subject.id,
                        status='active'
                    )
                    db.session.add(enrollment)
                    
                    # ALSO grant robust AuthSubjectAdmin rights to ALL sace_ subjects
                    from app.models.auth import AuthSubjectAdmin
                    sace_subjects = AuthSubject.query.filter(AuthSubject.slug.like('sace_%')).all()
                    for s_subj in sace_subjects:
                        admin_grant = AuthSubjectAdmin(email=email, subject_id=s_subj.id)
                        db.session.add(admin_grant)
                        
                    db.session.commit()
                    flash(f'Created SACE personnel account for {email} and granted SACE-wide admin access.', 'success')
                else:
                    flash('SACE subject not found in database.', 'error')"""

new_code = """                # Grant robust AuthSubjectAdmin rights to ALL sace_ subjects
                from app.models.auth import AuthSubjectAdmin
                sace_subjects = AuthSubject.query.filter(AuthSubject.slug.like('sace_%')).all()
                if not sace_subjects:
                    flash('No SACE subjects found in the database to grant access to.', 'error')
                else:
                    if sace_subject:
                        enrollment = UserEnrollment(
                            user_id=user.id,
                            subject_id=sace_subject.id,
                            status='active'
                        )
                        db.session.add(enrollment)
                        
                    for s_subj in sace_subjects:
                        admin_grant = AuthSubjectAdmin(email=email, subject_id=s_subj.id)
                        db.session.add(admin_grant)
                        
                    db.session.commit()
                    flash(f'Created SACE personnel account for {email} and granted SACE-wide admin access.', 'success')"""

text = text.replace(old_code, new_code)
with open('app/admin/security/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
