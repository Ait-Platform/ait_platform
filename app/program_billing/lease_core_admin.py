"""Platform-administrator-only interface over the existing private Core source."""
from io import BytesIO
from flask import abort,request,current_app,send_file,redirect,url_for,flash
from app.extensions import db
from .routes import billing_bp
from .lease_management import render_pm_template
from .lease_core_master import require_admin,publications,working_copies,prepare,publish,update_draft,read_object,DOCX
from . import lease_storage as storage


@billing_bp.route('/billing/admin/core-lease-master',methods=['GET','POST'],endpoint='lease_core_master')
def admin():
    require_admin();errors=[]
    if request.method=='POST':
        request.max_content_length=storage.MAX_BYTES+1024*1024
        try:
            action=request.form.get('action')
            if action=='prepare':prepare(request.files.get('working_docx'))
            elif action=='publish':publish(request.form.get('working_id'),request.files.get('published_pdf'))
            elif action=='update_draft':update_draft(int(request.form.get('property_id','')),request.form.get('draft_id'),request.form.get('version'))
            else:abort(400)
            flash('Core Lease Master action saved. Existing signed documents and other drafts are unchanged.','success')
            return redirect(url_for('billing_bp.lease_core_master'))
        except ValueError as error:errors.append(str(error))
        except Exception:
            from werkzeug.exceptions import HTTPException
            import sys
            if isinstance(sys.exc_info()[1],HTTPException):raise
            db.session.rollback();current_app.logger.exception('Core Lease Master action failed');errors.append('Private Core storage action failed. No new publication was confirmed. Check the service log; earlier versions remain available.')
    try:versions=publications();working=working_copies()
    except Exception:
        current_app.logger.exception('Core Lease registry unavailable')
        return render_pm_template('program_billing/lease_core_master.html',title='Core Lease Master',versions=[],working=[],errors=errors+['The private Core registry is unavailable. Verify existing R2 object read, write and list permissions.']),503
    return render_pm_template('program_billing/lease_core_master.html',title='Core Lease Master',versions=versions,working=working,errors=errors),400 if errors else 200


@billing_bp.get('/billing/admin/core-lease-master/<int:version>/<kind>',endpoint='lease_core_master_file')
def file(version,kind):
    require_admin();record=next((row for row in publications() if row['version']==version),None)
    if record is None or kind not in ('pdf','word'):abort(404)
    payload=read_object(record,'pdf');mime='application/pdf'
    if kind=='word':
        if record.get('source_docx'):payload=read_object(record['source_docx'],'docx')
        else:
            from .lease_sc_pdf import converted_word
            payload=converted_word(payload)
        mime=DOCX
    return send_file(BytesIO(payload),mimetype=mime,as_attachment=kind=='word',download_name='Core-Lease-v'+str(version)+('.docx' if kind=='word' else '.pdf'),max_age=0)


@billing_bp.after_request
def private(response):
    if request.endpoint in ('billing_bp.lease_core_master','billing_bp.lease_core_master_file'):
        response.headers['Cache-Control']='private, no-store';response.headers['X-Content-Type-Options']='nosniff';response.headers['Referrer-Policy']='same-origin'
    return response
