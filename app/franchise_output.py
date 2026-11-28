"""Franchise adapter for the AIT current-report Output controls and mail service."""
from datetime import datetime
from io import BytesIO
from zoneinfo import ZoneInfo
from flask import abort, flash, redirect, render_template, request, send_file, url_for
from flask_login import current_user
from werkzeug.exceptions import HTTPException
from sqlalchemy.exc import SQLAlchemyError
from app.extensions import db
from app import franchisees as flow
from app.utils.report_output import preview_token, verify_preview, send_report_once
from app.utils.pdf_render import html_to_pdf_bytes


def report_data(registration):
    rows,totals=flow.client_activity()
    return dict(registration_id=registration.id,name=registration.full_name,
        number=registration.franchisee_number,rows=[dict(row._mapping) for row in rows],totals=totals)


def reserve_email(nonce):
    # Reuse the existing account registration audit, with an atomic row lock.
    # Only append Output attempt/status events; never rewrite existing history.
    try:
        row=flow.current_registration(lock=True)
        if not row or not row.is_active:abort(403)
        if any(e.get('output_nonce')==nonce for e in row.history or []):
            abort(409,description='This preview was already submitted for email. Check the previous outcome before creating a new preview.')
        flow.audit(row,'output_email_attempted',current_user.id,output_nonce=nonce)
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback();abort(503,description='The email request could not be recorded. No email was attempted.')



def finish_email(nonce,status):
    try:
        row=flow.current_registration(lock=True)
        if not row:raise RuntimeError('Missing registration')
        flow.audit(row,'output_email_'+status,current_user.id,output_nonce=nonce)
        db.session.commit()
    except Exception:
        db.session.rollback()
        abort(503,description='The email outcome could not be recorded. Retry is blocked; check delivery before using a new preview.')


def output_page():
    registration=flow.current_registration()
    if not registration or not registration.is_active:abort(403,description='An active Franchise registration is required for Output.')
    args=request.form if request.method=='POST' else request.args
    data=None;token='';claim=None;error=None;status=200;html=None;filename=''
    # This is an all-time report: never silently honour an unimplemented filter.
    if any(args.get(key) for key in ('by_month','date_from','date_to')):
        abort(400,description='Date filters do not apply to this cumulative activity report.')
    if args.get('entity',str(registration.id))!=str(registration.id) or args.get('category','activity')!='activity' or args.get('selection','client_activity')!='client_activity':abort(404)
    wanted=args.get('preview')=='1' or args.get('format')=='pdf' or request.method=='POST'
    if wanted:
        try:
            data=report_data(registration)
            if request.method=='POST' or args.get('format')=='pdf':
                token=args.get('preview_token','');claim=verify_preview('franchise',current_user.id,data,token)
            else:
                generated=datetime.now(ZoneInfo('Africa/Johannesburg')).isoformat()
                token=preview_token('franchise',current_user.id,data,generated)
                claim=verify_preview('franchise',current_user.id,data,token)
            filename=claim['generated'][:10]+'_'+data['number']+'_Client-Activity-Report.pdf'
            html=render_template('franchisees/activity_document.html',report=data,generated=claim['generated'])
            if request.method=='POST' or args.get('format')=='pdf':
                if request.method=='POST' and args.get('action')!='email':abort(400,description='Use Send Email to submit the report.')
                try:
                    pdf=html_to_pdf_bytes(render_template('franchisees/activity_pdf.html',document_html=html))
                    if not pdf.startswith(b'%PDF-'):raise RuntimeError('Invalid PDF')
                except Exception:
                    abort(503,description='The report PDF could not be generated. No email was attempted. Please try Preview again or contact AIT Admin.')
                if request.method=='POST':
                    send_report_once(fields=args,pdf=pdf,filename=filename,label='Client Activity Report',
                        reserve=lambda:reserve_email(claim['nonce']),finish=lambda state:finish_email(claim['nonce'],state))
                    flash('Mail transport accepted the report. Inbox delivery is not confirmed.','success')
                    return redirect(url_for('franchisee_bp.output'))
                return send_file(BytesIO(pdf),mimetype='application/pdf',as_attachment=True,download_name=filename)
        except HTTPException as exc:
            error,status=exc.description,exc.code
            if args.get('format')=='pdf':return error,status
            if claim is None:data=None;token='';html=None
    return render_template('franchisees/output.html',registration=registration,report=data,document_html=html,
        preview_token=token,filename=filename,error=error,
        pdf_url=url_for('franchisee_bp.output',format='pdf',preview_token=token) if data else ''),status
