"""Versioned property master uploads; deliberately disconnected from generation."""
from io import BytesIO
import uuid
from flask import abort,request,redirect,url_for,flash,current_app,send_file
from flask_login import current_user
from sqlalchemy import text,inspect
from werkzeug.exceptions import HTTPException
from app.extensions import db
from app.program_billing.routes import billing_bp
from app.program_billing import lease_snapshot as snapshot
from app.program_billing.lease_management import render_pm_template

REVISION='pm_lease_master_001'


def readiness():
    inspector=inspect(db.session.connection())
    required={'id','property_id','version','original_filename','mime_type','storage_key','size_bytes','sha256','uploaded_by','uploaded_at','state','verified_at'}
    if not inspector.has_table('bil_property_lease_master') or not required<={c['name'] for c in inspector.get_columns('bil_property_lease_master')}:
        return 'Property master uploads require the approved pm_lease_master_001 migration. No migration was run automatically.'
    if not inspector.has_table('alembic_version') or REVISION not in db.session.execute(text('SELECT version_num FROM alembic_version')).scalars():
        return 'Property master migration pm_lease_master_001 is not recorded in Alembic history.'
    return None


def reserve_version(property_id,filename,mime,size,digest):
    """FOR UPDATE on the existing property serializes version allocation.

    Commit pending metadata before network I/O. Uniqueness is also enforced by
    PostgreSQL, and every state participates in MAX(version), preserving gaps.
    """
    snapshot.authorised_property(property_id,True)
    from app.program_billing import lease_storage as storage
    identifier=uuid.uuid4()
    version=db.session.execute(text('SELECT COALESCE(MAX(version),0)+1 FROM bil_property_lease_master WHERE property_id=:p'),{'p':property_id}).scalar_one()
    record=dict(id=identifier,property_id=property_id,version=version,original_filename=filename,mime_type=mime,
        storage_key=storage.master_key(property_id,identifier),size_bytes=size,sha256=digest,uploaded_by=int(current_user.id))
    db.session.execute(text("""INSERT INTO bil_property_lease_master
        (id,property_id,version,original_filename,mime_type,storage_key,size_bytes,sha256,uploaded_by)
        VALUES (:id,:property_id,:version,:original_filename,:mime_type,:storage_key,:size_bytes,:sha256,:uploaded_by)"""),record)
    db.session.commit()
    return record


def fail_record(record):
    db.session.rollback()
    db.session.execute(text("UPDATE bil_property_lease_master SET state='failed' WHERE id=:id AND uploaded_by=:u AND state='pending'"),{'id':record['id'],'u':current_user.id})
    db.session.commit()


@billing_bp.route('/billing/leases/property/<int:property_id>/masters',methods=['GET','POST'],endpoint='lease_property_masters')
@snapshot.endpoint
def masters(property_id):
    from app.program_billing import lease_storage as storage
    prop=snapshot.authorised_property(property_id)
    if request.method=='POST':request.max_content_length=storage.MAX_BYTES+1024*1024
    draft=None
    if request.values.get('draft_id'):
        snapshot.require_particulars_schema();draft=snapshot.draft_row(property_id,request.values['draft_id'])
    reason=readiness();storage_reason=None
    try:storage.destination()
    except ValueError as error:storage_reason=str(error)
    errors=[];status=200
    if request.method=='POST':
        if reason or storage_reason:abort(503,description=reason or storage_reason)
        upload=request.files.get('master_document')
        if upload is None:errors.append('Select a DOCX master document.')
        else:
            try:filename,mime,data,digest=storage.validate_master(upload)
            except ValueError as error:errors.append(str(error))
        if errors:status=400
        else:
            record=reserve_version(property_id,filename,mime,len(data),digest)
            try:
                storage.upload(record['storage_key'],data,mime)
                if storage.retrieve_master(record)!=data:raise ValueError('Read-back mismatch')
            except Exception as error:
                fail_record(record)
                current_app.logger.warning('Property master storage verification failed error=%s',type(error).__name__)
                errors.append('Upload or storage verification failed. This version is not current; earlier verified masters are unchanged.')
                status=503
            else:
                try:
                    # Recheck after network I/O; reassignment cannot race this commit.
                    snapshot.authorised_property(property_id,True)
                except HTTPException:
                    fail_record(record);raise
                updated=db.session.execute(text("UPDATE bil_property_lease_master SET state='available',verified_at=now() WHERE id=:id AND state='pending' RETURNING id"),{'id':record['id']}).scalar_one_or_none()
                if updated is None:abort(409,description='This upload was not published; reopen the property master history.')
                db.session.commit()
                flash('Master document uploaded and verified. View Lease and Generate Word still use the existing Stage 5 master.','success')
                return redirect(url_for('billing_bp.lease_property_masters',property_id=property_id,draft_id=draft['id'] if draft else None))
    versions=[]
    if not reason:
        # Storage keys are never passed to the page.
        versions=db.session.execute(text("""SELECT id,version,original_filename,mime_type,size_bytes,sha256,uploaded_by,uploaded_at,state,verified_at
            FROM bil_property_lease_master WHERE property_id=:p ORDER BY version DESC"""),{'p':property_id}).mappings().all()
    current=next((row for row in versions if row['state']=='available' and row['verified_at'] is not None),None)
    return render_pm_template('program_billing/lease_property_masters.html',title='Upload Master Document',property=prop,
        lease_draft=draft,versions=versions,current_master=current,unavailable=reason,storage_reason=storage_reason,
        errors=errors,upload_enabled=not(reason or storage_reason)),status


@billing_bp.get('/billing/leases/property/<int:property_id>/masters/<uuid:master_id>/download',endpoint='lease_property_master_download')
@snapshot.endpoint
def download(property_id,master_id):
    snapshot.authorised_property(property_id)
    reason=readiness()
    if reason:abort(503,description=reason)
    record=db.session.execute(text("SELECT * FROM bil_property_lease_master WHERE id=:id AND property_id=:p AND state='available' AND verified_at IS NOT NULL"),{'id':master_id,'p':property_id}).mappings().first()
    if record is None:abort(404)
    from app.program_billing import lease_storage as storage
    try:payload=storage.retrieve_master(record)
    except Exception as error:
        current_app.logger.warning('Property master download verification failed error=%s',type(error).__name__)
        abort(503,description='The private master document could not be retrieved or verified.')
    response=send_file(BytesIO(payload),mimetype=record['mime_type'],as_attachment=True,
        download_name=record['original_filename'],conditional=False,etag=False,max_age=0)
    response.headers['Content-Security-Policy']="sandbox; default-src 'none'"
    return response


@billing_bp.after_request
def protect_masters(response):
    if request.endpoint in ('billing_bp.lease_property_masters','billing_bp.lease_property_master_download'):
        response.headers['Cache-Control']='private, no-store'
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Referrer-Policy']='no-referrer'
    return response
