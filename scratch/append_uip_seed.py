from flask import flash, redirect, url_for

@uip_bp.route("/_seed")
def seed_uip_live():
    from app.extensions import db
    from app.models.auth import AuthSubject
    subj = AuthSubject.query.filter_by(slug='uip').first()
    if not subj:
        subj = AuthSubject(
            slug='uip',
            name='UIP Platform',
            program_type='B2B',
            show_on_welcome=True,
            about_endpoint='uip_bp.uip_start',
            processor_default='yoco'
        )
        db.session.add(subj)
    else:
        subj.show_on_welcome = True
        subj.about_endpoint = 'uip_bp.uip_start'
        subj.processor_default = 'yoco'
    db.session.commit()
    flash("UIP module seeded into live database!", "success")
    return redirect(url_for('admin_bp.modules_control'))
