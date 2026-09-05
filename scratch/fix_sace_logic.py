import re

with open('app/admin/security/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_logic = """                # Grant robust AuthSubjectAdmin rights to ALL sace_ subjects
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

new_logic = """                # Grant robust AuthSubjectAdmin rights to ONLY the selected SACE subject
                from app.models.auth import AuthSubjectAdmin
                assigned_slug = request.form.get('assigned_subject_slug')
                
                target_subject = AuthSubject.query.filter_by(slug=assigned_slug).first()
                if not target_subject:
                    flash(f'The selected SACE activity ({assigned_slug}) was not found in the database.', 'error')
                else:
                    # Optional: still enroll them in sace_hub for legacy dashboard access if needed
                    if sace_subject:
                        enrollment = UserEnrollment(
                            user_id=user.id,
                            subject_id=sace_subject.id,
                            status='active'
                        )
                        db.session.add(enrollment)
                        
                    admin_grant = AuthSubjectAdmin(email=email, subject_id=target_subject.id)
                    db.session.add(admin_grant)
                        
                    db.session.commit()
                    flash(f'Created SACE personnel account for {email} and granted access strictly to {target_subject.name}.', 'success')"""

text = text.replace(old_logic, new_logic)

with open('app/admin/security/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
