"""Browser-local report destinations; no filesystem paths or files on the server."""
from flask import request, current_app
from flask_login import current_user, login_required
from sqlalchemy import text
from app.extensions import db
from .routes import billing_bp
from .lease_management import require_pm_user, render_pm_template
from .metsoa_setup_safety import property_scope
from .utilities_output_reports import CATALOG, SOURCES, report_destinations


@billing_bp.get('/billing/directory-register', endpoint='directory_register')
@login_required
def directory_register():
    require_pm_user()
    selected = request.args.get('property_id')
    if selected:
        selected = property_scope(selected)
    properties = [dict(row) for row in db.session.execute(text(
        'SELECT id,name FROM bil_property WHERE manager_id=:u ORDER BY name,id'),
        dict(u=current_user.id)).mappings()]
    selected = next((p for p in properties if p['id'] == selected), properties[0] if properties else None)
    response = current_app.make_response(render_pm_template('program_billing/directory_register.html',
        properties=properties, selected_property=selected,
        directory_email_to=getattr(current_user, 'email', '') or '',
        directory_context=dict(user=str(current_user.id), property=str(selected['id']) if selected else ''),
        directory_reports=report_destinations()))
    response.headers['Cache-Control'] = 'private, no-store'
    return response

@billing_bp.post('/billing/directory-register/email', endpoint='directory_email')
@login_required
def directory_email():
    """Explicitly email supplied historical bytes; never render a replacement report."""
    import hashlib
    import json
    from uuid import UUID
    from email_validator import validate_email, EmailNotValidError
    from flask import jsonify
    from werkzeug.exceptions import HTTPException
    from app.utils.mailer import send_pdf_email

    require_pm_user()
    # Bound multipart parsing as well as the attachment read. Browser FormData
    # supplies Content-Length; unknown/unbounded bodies are not accepted here.
    if not request.content_length or request.content_length > 11 * 1024 * 1024:
        return jsonify(message='Email attachments must be 10 MB or smaller.'), 413
    property_id = property_scope(request.form.get('property_id'))
    report = request.form.get('report')
    titles = {code: title for items in CATALOG.values() for code, title in items}
    if report not in titles:
        return jsonify(message='Select an existing PM report destination.'), 400
    try:
        address = validate_email(request.form.get('email_to', ''), check_deliverability=False).normalized
        request_id = str(UUID(request.form.get('request_id', '')))
    except (EmailNotValidError, ValueError, TypeError, AttributeError):
        return jsonify(message='Enter a valid recipient and reopen the saved PDF if necessary.'), 400
    upload = request.files.get('document')
    filename = upload.filename if upload else ''
    if not filename or len(filename) > 255 or any(c in filename for c in '/\\\r\n\x00') or not filename.lower().endswith('.pdf'):
        return jsonify(message='Select an original saved PDF with a valid filename.'), 400
    pdf = upload.stream.read(10 * 1024 * 1024 + 1)
    if len(pdf) > 10 * 1024 * 1024:
        return jsonify(message='Email attachments must be 10 MB or smaller.'), 413
    if not pdf.startswith(b'%PDF-'):
        return jsonify(message='The selected document is not a PDF.'), 400
    transport = current_app.extensions.get('mail')
    if current_app.config.get('MAIL_SUPPRESS_SEND') or getattr(transport, 'suppress', False):
        return jsonify(message='Email is unavailable; delivery was not attempted.'), 503
    key = hashlib.sha256(json.dumps([current_user.id, property_id, request_id, address]).encode()).hexdigest()
    details = dict(request_key=key, status='attempted', report=report, sha256=hashlib.sha256(pdf).hexdigest())
    params = dict(p=property_id, u=current_user.id, key=key)
    try:
        db.session.execute(text('SELECT pg_advisory_xact_lock(hashtextextended(:key,0))'), dict(key='pm-history-email:' + key))
        exists = db.session.execute(text("""SELECT id FROM bil_utilities_capture_audit
            WHERE property_id=:p AND actor_id=:u AND kind='history_email'
            AND after_data->>'request_key'=:key LIMIT 1"""), params).scalar()
        if exists:
            db.session.rollback()
            return jsonify(message='This email was already submitted. Check the previous outcome before an intentional new request.'), 409
        db.session.execute(text("""INSERT INTO bil_utilities_capture_audit
            (property_id,kind,record_id,actor_id,after_data)
            VALUES(:p,'history_email',:p,:u,CAST(:details AS json))"""), dict(params, details=json.dumps(details)))
        db.session.commit()
    except HTTPException:
        db.session.rollback()
        raise
    except Exception:
        db.session.rollback()
        return jsonify(message='Email could not be recorded safely; no delivery was attempted.'), 503
    try:
        if not send_pdf_email(address, 'Property Management - ' + titles[report],
                              'Please find the original saved Property Management document attached.', pdf, filename):
            raise RuntimeError('SMTP acceptance not confirmed')
        details['status'] = 'accepted'
        db.session.execute(text("""INSERT INTO bil_utilities_capture_audit
            (property_id,kind,record_id,actor_id,after_data)
            VALUES(:p,'history_email',:p,:u,CAST(:details AS json))"""), dict(params, details=json.dumps(details)))
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify(message='Email delivery was not confirmed. Check the previous outcome before attempting a new send.'), 503
    return jsonify(message='The mail service accepted the original saved PDF for delivery.')
