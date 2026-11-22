"""ProTrade-only local-folder adapters; no token or financial operations."""
import hashlib
from uuid import UUID
from flask import abort,current_app,jsonify,render_template,request,session
from flask_login import current_user
from sqlalchemy.exc import IntegrityError
from app.extensions import db
from app.models.mechanic import MechShop,ProTradeOutputEmail

TYPES={'quote':'Quotes','invoice':'Invoices','statement':'Statements'}
LEGACY={'legacy_quotes':'Quotes & Invoices','legacy_boq':'BOQ','legacy_soa':'SOA'}

def local_reports(shops):
    from app.protrade_output import folder_reports
    original=folder_reports(shops);result=[]
    for shop in shops:
        if shop.user_id!=current_user.id:abort(403)
        previous=next(row for row in original if row['code']==str(shop.id)+':quote')
        business=previous['path'][1]+' (business '+str(shop.id)+')'
        for kind,title in dict(TYPES,**LEGACY).items():
            result.append(dict(code=str(shop.id)+':'+kind,title=title,source=previous['source'],
                path=[previous['path'][0],previous['path'][1],title],owner=previous['owner'],
                directories=[previous['path'][0],business,('Earlier / ' if kind in LEGACY else '')+title],
                legacy=kind in LEGACY,shop=str(shop.id),kind=kind))
    return result

def owned_shops():
    return MechShop.query.filter_by(user_id=current_user.id).order_by(MechShop.business_name,MechShop.id).all()

def access_context():
    response=jsonify(user=str(current_user.id),shops=[str(shop.id) for shop in owned_shops()])
    response.headers['Cache-Control']='private, no-store'
    return response

def folders_page():
    shops=owned_shops();raw=request.args.get('shop_id',session.get('active_shop_id'));shop=None
    if raw is not None:
        try:identifier=int(raw)
        except (TypeError,ValueError):abort(404)
        shop=next((row for row in shops if row.id==identifier),None)
        if not shop:abort(404)
    response=current_app.make_response(render_template('program_mechanic/folders.html',shop=shop,shops=shops,
        directory_reports=local_reports(shops),protrade_menu_active='folders'))
    response.headers['Cache-Control']='private, no-store'
    return response

def email_history():
    from email_validator import validate_email,EmailNotValidError
    from app.utils.mailer import send_pdf_email
    if not request.content_length or request.content_length>11*1024*1024:return jsonify(message='Attachments must be 10 MB or smaller.'),413
    if request.form.get('property_id')!='protrade':abort(400)
    try:
        identifier,kind=request.form.get('report','').split(':',1);identifier=int(identifier)
        request_id=str(UUID(request.form.get('request_id','')))
        address=validate_email(request.form.get('email_to',''),check_deliverability=False).normalized
    except (ValueError,TypeError,EmailNotValidError):return jsonify(message='Select an authorised business and valid recipient.'),400
    shop=MechShop.query.filter_by(id=identifier,user_id=current_user.id).first()
    if not shop:abort(404)
    if kind not in dict(TYPES,**LEGACY) or len(address)>150:return jsonify(message='Select a supported document type and recipient.'),400
    upload=request.files.get('document');name=upload.filename if upload else ''
    if not name or len(name)>255 or any(c in name for c in '/\\\r\n\x00') or not name.lower().endswith('.pdf'):return jsonify(message='Select an original saved PDF.'),400
    pdf=upload.stream.read(10*1024*1024+1)
    if len(pdf)>10*1024*1024:return jsonify(message='Attachments must be 10 MB or smaller.'),413
    if not pdf.startswith(b'%PDF-'):return jsonify(message='The saved document is not a PDF.'),400
    transport=current_app.extensions.get('mail')
    if current_app.config.get('MAIL_SUPPRESS_SEND') or getattr(transport,'suppress',False):return jsonify(message='Email is unavailable; delivery was not attempted.'),503
    key=hashlib.sha256(f'history:{current_user.id}:{shop.id}:{request_id}:{address}'.encode()).hexdigest()
    receipt=ProTradeOutputEmail(request_key=key,user_id=current_user.id,shop_id=shop.id,debtor_id=None,
        selection='history:'+kind+':'+request_id,recipient=address,status='attempted')
    try:db.session.add(receipt);db.session.commit()
    except IntegrityError:
        db.session.rollback()
        if ProTradeOutputEmail.query.filter_by(request_key=key,user_id=current_user.id,shop_id=shop.id).first():return jsonify(message='This send was already submitted. Check its previous outcome.'),409
        return jsonify(message='The email attempt could not be recorded. No delivery attempted.'),503
    except Exception:
        db.session.rollback();return jsonify(message='Email could not be recorded. Apply the approved local-files migration before enabling historical email.'),503
    try:
        if not send_pdf_email(address,shop.business_name+' - saved document','Original saved ProTrade document attached.',pdf,name):raise RuntimeError('SMTP not confirmed')
        receipt.status='accepted';db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify(message='Email was not confirmed. Automatic retry is blocked; check delivery first.'),503
    return jsonify(message='The mail service accepted the original saved PDF for delivery.')
