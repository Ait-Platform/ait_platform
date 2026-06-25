from flask import render_template, request, flash, redirect, url_for, session
from flask_login import login_required, current_user
from . import hds_bp
from app.models.hds import HdsOrganization, HdsClaim
from app.extensions import db

@hds_bp.route('/about')
def about():
    """Public landing page for the Healthcare Data Switch program"""
    return render_template('program_hds/about.html')

@hds_bp.route('/dashboard', methods=['GET', 'POST'])
@login_required
def dashboard():
    if request.method == 'POST':
        flash('Claim submitted successfully! Waiting for clearinghouse response...', 'success')
        return redirect(url_for('hds_bp.dashboard'))
    """Secure dashboard for enrolled users"""
    # Verify the user actually has enrolled access to the 'hds' program
    subjects_access = session.get('subjects_access', {})
    hds_access = subjects_access.get('hds')
    
    if hds_access not in ['enrolled', 'admin']:
        flash('You must be enrolled to access the Healthcare Data Switch dashboard.', 'error')
        return redirect(url_for('public_bp.welcome'))
        
    from app.models.practice_crm import CrmEnquiry
    
    # Fetch all enquiries so the mock data is visible to the current user
    enquiries = CrmEnquiry.query.order_by(CrmEnquiry.created_at.desc()).all()
    patients = []
    seen = set()
    for e in enquiries:
        key = e.patient_name.lower().strip()
        if key not in seen:
            seen.add(key)
            patients.append({
                'name': e.patient_name,
                'id_no': e.patient_id_no or '',
                'medical_aid': e.medical_aid or '',
                'medical_aid_no': e.medical_aid_no or ''
            })
            
    return render_template('program_hds/dashboard.html', patients=patients)

@hds_bp.route('/api/patients/search')
@login_required
def search_patients():
    from flask import jsonify
    from app.models.practice_crm import CrmEnquiry
    
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify([])
        
    enquiries = CrmEnquiry.query.filter_by(created_by_id=current_user.id)\
        .filter((CrmEnquiry.patient_name.ilike(f'%{query}%')) | (CrmEnquiry.patient_id_no.ilike(f'%{query}%')))\
        .order_by(CrmEnquiry.created_at.desc())\
        .limit(20).all()
        
    results = []
    seen = set()
    for e in enquiries:
        key = e.patient_name.lower().strip()
        if key not in seen:
            seen.add(key)
            results.append({
                'name': e.patient_name,
                'id_no': e.patient_id_no or '',
                'medical_aid': e.medical_aid or '',
                'medical_aid_no': e.medical_aid_no or ''
            })
            
    return jsonify(results)

@hds_bp.route('/start-trial', methods=['GET', 'POST'])
def start_trial():
    """Start 30-day free trial for HDS (Fixed 150 ZAR/m post-trial)"""
    from app.models.auth import UserEnrollment, AuthSubject
    from datetime import datetime, timedelta
    
    if not current_user.is_authenticated:
        return redirect(url_for('auth_bp.register', subject='hds'))
        
    hds_subject = AuthSubject.query.filter_by(slug='hds').first()
    if not hds_subject:
        flash("HDS program is currently unavailable.", "error")
        return redirect(url_for('public_bp.welcome'))

    enr = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=hds_subject.id).first()
    
    if not enr:
        enr = UserEnrollment(
            user_id=current_user.id,
            subject_id=hds_subject.id,
            status="active",
            country_code="ZA",
            local_currency="ZAR",
            local_amount_cents=15000,
            zar_amount_cents=15000,
            trial_count=1,
            trial_end=datetime.utcnow() + timedelta(days=30),
            started_at=datetime.utcnow()
        )
        db.session.add(enr)
    else:
        enr.status = 'active'
        if not enr.trial_end:
            enr.trial_end = datetime.utcnow() + timedelta(days=30)
            
    db.session.commit()
    
    # Update session access immediately so they don't have to relogin
    access = session.get('subjects_access', {})
    access['hds'] = 'enrolled'
    session['subjects_access'] = access
    session.modified = True
    
    flash("Your 30-Day Free Trial for HDS has started!", "success")
    return redirect(url_for('hds_bp.dashboard'))
