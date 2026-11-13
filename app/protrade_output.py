"""Central ProTrade Output: read-only report adapters and explicit email attempts."""
import hashlib
import json
from datetime import datetime,timezone
from decimal import Decimal
from io import BytesIO
from uuid import uuid4
from zoneinfo import ZoneInfo
from flask import abort,current_app,flash,redirect,render_template,request,send_file,session,url_for,jsonify
from flask_login import current_user
from itsdangerous import URLSafeTimedSerializer,BadSignature,SignatureExpired
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import HTTPException
from werkzeug.utils import secure_filename
from app.extensions import db
from app.models.mechanic import (MechShop,MechClient,MechVehicle,MechJobCard,MechInvoice,
                                MechLaborLine,MechPartLine,MechCatalogPart,ProTradeBoqItem,
                                ProTradeBoqQuote,ProTradeOutputEmail)
from app.models.debtors import Debtor,DebtorLedger

LABELS={'quote':'Quotations','invoice':'Invoices','statement':'Debtor Statements','price_list':'Price Lists'}


def price_items(shop):
    from app.program_mechanic.routes import selected_price_list
    return selected_price_list(shop)


def owned_debtor(shop,identifier):
    try:identifier=int(identifier)
    except (ValueError,TypeError):abort(404)
    debtor=Debtor.query.filter_by(id=identifier,shop_id=shop.id,user_id=current_user.id).first()
    if not debtor:abort(404)
    return debtor


def job_debtor(shop,job):
    if job.debtor_id is not None:
        debtor=owned_debtor(shop,job.debtor_id)
        if debtor.reference_id!=job.vehicle.client_id:abort(404)
        return debtor
    linked=Debtor.query.filter_by(shop_id=shop.id,user_id=current_user.id,reference_id=job.vehicle.client_id).limit(2).all()
    return linked[0] if len(linked)==1 else None


def catalog(shop):
    entries=[]
    if shop.trade=='carpentry':
        for quote in ProTradeBoqQuote.query.filter_by(shop_id=shop.id,user_id=current_user.id,trade='carpentry').order_by(ProTradeBoqQuote.id.desc()).all():
            if quote.debtor_id is not None:
                debtor=Debtor.query.filter_by(id=quote.debtor_id,shop_id=shop.id,user_id=current_user.id).first()
                if not debtor or debtor.reference_id!=quote.client_id:continue
            if quote.client_id is not None:
                client=MechClient.query.filter_by(id=quote.client_id,user_id=current_user.id).first()
                if not client or client.shop_id not in (None,shop.id):continue
            entries.append(dict(key=f'quote:boq-{quote.id}',kind='quote',label=quote.quote_number+' — '+quote.client_name,source=quote))
        if ProTradeBoqItem.query.filter_by(shop_id=shop.id,user_id=current_user.id,trade='carpentry').first():
            entries.append(dict(key='price_list:all',kind='price_list',label='Carpentry BOQ Price List',source=None))
    else:
        if (shop.trade or 'mechanic')=='mechanic':
            jobs=MechJobCard.query.join(MechVehicle).join(MechClient).filter(MechJobCard.shop_id==shop.id,MechClient.user_id==current_user.id).order_by(MechJobCard.id.desc()).all()
            for job in jobs:
                invoices=MechInvoice.query.filter_by(job_card_id=job.id).order_by(MechInvoice.id).all()
                if job.status!='Billed':
                    entries.append(dict(key=f'quote:job-{job.id}',kind='quote',label=job.job_number+' — '+job.vehicle.client.name,source=job))
                elif not invoices:
                    entries.append(dict(key=f'invoice:billed-{job.id}',kind='invoice',label=job.job_number+' — Existing billed document',source=job))
                for invoice in invoices:
                    entries.append(dict(key=f'invoice:stored-{invoice.id}',kind='invoice',label=job.job_number+f' — Invoice record {invoice.id}',source=(invoice,job)))
        if price_items(shop):entries.append(dict(key='price_list:all',kind='price_list',label='Price List',source=None))
    for debtor in Debtor.query.filter_by(shop_id=shop.id,user_id=current_user.id).order_by(Debtor.name,Debtor.id).all():
        if DebtorLedger.query.filter_by(debtor_id=debtor.id).first():
            entries.append(dict(key=f'statement:debtor-{debtor.id}',kind='statement',label=debtor.name+' — Saved ledger',source=debtor))
    return entries


def document_date(value):
    if value is None:return ''
    aware=value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(ZoneInfo('Africa/Johannesburg')).date().isoformat()


def contact(source):
    return dict(name=source.name,phone=source.phone or '',email=source.email or '',address=source.address or '')


def report_data(shop,entry,recipient_id=None):
    kind=entry['kind'];source=entry['source'];debtor=None
    data=dict(kind=kind,title=LABELS[kind].rstrip('s'),reference=entry['label'],recipient=None,
              columns=[],rows=[],totals=[],note='',date='')
    if entry['key'].startswith('quote:boq-'):
        if source.debtor_id is not None:
            debtor=owned_debtor(shop,source.debtor_id)
            if source.client_id!=debtor.reference_id:abort(404)
        data.update(reference=source.quote_number,date=document_date(source.created_at),
                    recipient=dict(name=source.client_name,phone=source.client_phone or '',email=source.client_email or '',address=source.client_address or ''),
                    columns=['BOQ Code','Description','Unit','Quantity','Material R/unit','Labour R/unit','Total R'],
                    rows=[[line['code'],line['description'],line['unit'],line['quantity'],line['material_rate'],line['labour_rate'],line['total']] for line in source.items],
                    totals=[('Total (R)',format(source.grand_total,'.2f'))])
    elif kind in ('quote','invoice'):
        invoice,job=source if isinstance(source,tuple) else (None,source)
        debtor=job_debtor(shop,job)
        data.update(reference=job.job_number,date=document_date(invoice.created_at if invoice else job.created_at),recipient=contact(job.vehicle.client))
        if invoice:
            data.update(columns=['Description','Amount (R)'],rows=[['Subtotal',format(invoice.subtotal or 0,'.2f')],['Tax',format(invoice.tax or 0,'.2f')]],
                        totals=[('Total (R)',format(invoice.total or 0,'.2f'))],note='')
        else:
            parts=MechPartLine.query.filter_by(job_card_id=job.id).all()
            labour=MechLaborLine.query.filter_by(job_card_id=job.id).all()
            rows=[];subtotal=0
            for line in parts:
                amount=(line.quantity or 0)*(line.markup_price or 0);subtotal+=amount
                rows.append([line.description or '',str(line.quantity or 0),format(line.markup_price or 0,'.2f'),format(amount,'.2f')])
            for line in labour:
                amount=(line.hours or 0)*(line.rate_per_hour or 0);subtotal+=amount
                rows.append([line.description or '',str(line.hours or 0),format(line.rate_per_hour or 0,'.2f'),format(amount,'.2f')])
            vat=subtotal*((job.vat_rate or 0)/100)
            data.update(columns=['Description','Quantity / Hours','Rate (R)','Total (R)'],rows=rows,
                        totals=[('Subtotal (R)',format(subtotal,'.2f')),('VAT (R)',format(vat,'.2f')),('Total (R)',format(subtotal+vat,'.2f'))])
    elif kind=='statement':
        debtor=owned_debtor(shop,source.id)
        balance=0;rows=[]
        for line in DebtorLedger.query.filter_by(debtor_id=debtor.id).order_by(DebtorLedger.txn_date,DebtorLedger.id).all():
            if line.kind not in ('debit','credit'):abort(409,description='The saved ledger contains an unsupported entry. Review the existing ledger before exporting.')
            balance+=line.amount if line.kind=='debit' else -line.amount
            rows.append([line.txn_date.isoformat(),line.description,format(Decimal(line.amount)/100,'.2f') if line.kind=='debit' else '',
                         format(Decimal(line.amount)/100,'.2f') if line.kind=='credit' else '',format(Decimal(balance)/100,'.2f')])
        data.update(reference=f'Debtor-{debtor.id}',recipient=contact(debtor),columns=['Date','Description','Debit (R)','Credit (R)','Balance (R)'],rows=rows,
                    totals=[('Recorded ledger balance (R)',format(Decimal(balance)/100,'.2f'))],note='Transactions recorded for this debtor.')
    else:
        if shop.trade=='carpentry':
            data.update(reference='Carpentry-BOQ',columns=['BOQ Code','Description','Unit','Material R/unit','Labour R/unit','Unit Rate (R)'],
                        rows=[[item.code,item.description,item.unit,format(item.material_rate,'.2f'),format(item.labour_rate,'.2f'),format(item.total_rate,'.2f')]
                              for item in ProTradeBoqItem.query.filter_by(shop_id=shop.id,user_id=current_user.id,trade='carpentry').order_by(ProTradeBoqItem.code).all()])
        else:
            data.update(reference='Price-List',columns=['Item','Category','Unit Price (R)'],rows=[[item.part_name,item.category or '',format(item.default_price or 0,'.2f')] for item in price_items(shop)])
        if recipient_id:debtor=owned_debtor(shop,recipient_id)
    if recipient_id and (not debtor or str(debtor.id)!=str(recipient_id)):
        abort(400,description='Select the saved debtor linked to this document.')
    email=dict(id=debtor.id,**contact(debtor)) if debtor else None
    issuer=dict(id=shop.id,name=shop.business_name or '',trade=shop.trade or 'mechanic',address=shop.address or '',phone=shop.phone or '',email=shop.email or '',tax_number=shop.tax_number or '')
    return dict(document=data,issuer=issuer,email_recipient=email)


def signer():
    return URLSafeTimedSerializer(current_app.secret_key,salt='protrade-output-v1')


def fingerprint(data):
    return hashlib.sha256(json.dumps(data,sort_keys=True,default=str,separators=(',',':')).encode()).hexdigest()


def make_token(shop,selection,recipient_id,data):
    return signer().dumps(dict(user=current_user.id,shop=shop.id,selection=selection,recipient=str(recipient_id or ''),digest=fingerprint(data),
                              generated=datetime.now(ZoneInfo('Africa/Johannesburg')).isoformat(),nonce=uuid4().hex))


def verify_token(token,shop,selection,recipient_id,data):
    try:claim=signer().loads(token or '',max_age=1800)
    except (BadSignature,SignatureExpired):abort(400,description='This preview is invalid or expired. Click Preview again.')
    if claim.get('user')!=current_user.id or claim.get('shop')!=shop.id or claim.get('selection')!=selection or claim.get('recipient')!=str(recipient_id or ''):
        abort(400,description='The selected shop or document changed. Click Preview again.')
    if claim.get('digest')!=fingerprint(data):abort(409,description='The document or debtor details changed after preview. Click Preview again.')
    return claim


def pdf_bytes(data,claim):
    from app.utils.pdf_render import html_to_pdf_bytes
    return html_to_pdf_bytes(render_template('program_mechanic/output_pdf.html',**data,generated=claim['generated']))


def dispatch_once(shop,selection,data,claim,pdf):
    from email_validator import validate_email,EmailNotValidError
    from app.utils.mailer import send_pdf_email
    recipient=data['email_recipient']
    if not recipient or not recipient['email']:abort(400,description='This document needs a linked debtor with a saved email address.')
    try:address=validate_email(recipient['email'],check_deliverability=False).normalized
    except EmailNotValidError:abort(400,description='Update the debtor with a valid saved email address first.')
    key=hashlib.sha256(f'{current_user.id}:{shop.id}:{selection}:{claim["nonce"]}:{address}'.encode()).hexdigest()
    receipt=ProTradeOutputEmail(request_key=key,user_id=current_user.id,shop_id=shop.id,debtor_id=recipient['id'],selection=selection,recipient=address,status='attempted')
    try:
        db.session.add(receipt);db.session.commit()
    except IntegrityError:
        db.session.rollback()
        if ProTradeOutputEmail.query.filter_by(request_key=key,user_id=current_user.id,shop_id=shop.id).first():
            abort(409,description='This email confirmation has already been submitted. It will not be sent again. Check the prior outcome before intentionally creating a new preview.')
        abort(503,description='The email request could not be recorded. No email was attempted.')
    except Exception:
        db.session.rollback();abort(503,description='The email request could not be recorded. No email was attempted.')
    try:
        transport=current_app.extensions.get('mail')
        if current_app.config.get('MAIL_SUPPRESS_SEND') or getattr(transport,'suppress',False):raise RuntimeError('Mail is suppressed')
        sent=send_pdf_email(address,shop.business_name+' — '+data['document']['title'],'Please find the requested document attached.',pdf,data['filename'])
        if not sent:raise RuntimeError('SMTP acceptance not confirmed')
    except Exception:
        try:
            receipt.status='uncertain';db.session.commit()
        except Exception:
            db.session.rollback()
        abort(503,description='Email delivery was not confirmed. Automatic retry is blocked to prevent duplicates. Check delivery before intentionally using a new preview.')
    try:
        receipt.status='accepted';db.session.commit()
    except Exception:
        db.session.rollback()
        abort(503,description='Mail transport returned success, but recording the outcome failed. Retry is blocked to prevent duplicates. Check delivery before using a new preview.')


def output_page():
    args=request.form if request.method=='POST' else request.args
    shops=MechShop.query.filter_by(user_id=current_user.id).order_by(MechShop.business_name,MechShop.id).all()
    raw_shop=args.get('shop_id',session.get('active_shop_id'))
    shop=None
    if raw_shop is not None:
        try:identifier=int(raw_shop)
        except (ValueError,TypeError):abort(404)
        shop=MechShop.query.filter_by(id=identifier,user_id=current_user.id).first()
        if not shop:abort(404)
    choices=catalog(shop) if shop else []
    public_choices=[{key:item[key] for key in ('key','kind','label')} for item in choices]
    selected=args.get('selection','');recipient_id=args.get('recipient_id','')
    debtors=Debtor.query.filter_by(shop_id=shop.id,user_id=current_user.id).order_by(Debtor.name,Debtor.id).all() if shop else []
    data=None;token='';claim=None;error=None;status=200
    wanted=args.get('preview')=='1' or args.get('format')=='pdf' or request.method=='POST'
    if wanted:
        try:
            if not shop:abort(400,description='Select a shop first.')
            entry=next((item for item in choices if item['key']==selected),None)
            if not entry:abort(404)
            data=report_data(shop,entry,recipient_id)
            if request.method=='POST' or args.get('format')=='pdf':
                token=args.get('preview_token','');claim=verify_token(token,shop,selected,recipient_id,data)
            else:
                token=make_token(shop,selected,recipient_id,data);claim=signer().loads(token)
            data['filename']=f'{claim["generated"][:10]}_Shop-{shop.id}_{entry["kind"]}_{secure_filename(data["document"]["reference"])[:70]}_{claim["nonce"][:10]}.pdf'
            if request.method=='POST' and (args.get('action')!='email' or args.get('confirm_email')!='yes'):
                abort(400,description='Confirm sending to the saved debtor email address before sending.')
            if request.method=='POST' or args.get('format')=='pdf':
                try:
                    pdf=pdf_bytes(data,claim)
                    if not pdf.startswith(b'%PDF-'):raise RuntimeError('PDF renderer did not return a PDF')
                except Exception:
                    current_app.logger.exception('ProTrade Output PDF failed')
                    abort(503,description='The PDF could not be generated. No report was saved or emailed. Contact the administrator to check the existing PDF renderer.')
                if request.method=='POST':
                    dispatch_once(shop,selected,data,claim,pdf)
                    flash('Mail transport accepted the document. Inbox delivery is not confirmed.','success')
                    return redirect(url_for('mechanic_bp.protrade_output',shop_id=shop.id))
                return send_file(BytesIO(pdf),mimetype='application/pdf',as_attachment=True,download_name=data['filename'])
        except HTTPException as exc:
            if args.get('format')=='pdf':return jsonify(error=exc.description),exc.code
            error,status=exc.description,exc.code
            if claim is None:
                data=None;token=''
    directory_reports=[dict(code=key,title=value,source=secure_filename(f'Shop-{shop.id}-{shop.business_name}')[:80] or f'Shop-{shop.id}') for key,value in LABELS.items()] if shop else []
    pdf_url=url_for('mechanic_bp.protrade_output',shop_id=shop.id,selection=selected,recipient_id=recipient_id,format='pdf',preview_token=token) if data else ''
    return render_template('program_mechanic/output.html',shop=shop,shops=shops,choices=public_choices,labels=LABELS,selection=selected,
                           debtors=debtors,recipient_id=recipient_id,report_data=data,preview_token=token,generated=claim['generated'] if claim else '',
                           pdf_url=pdf_url,directory_reports=directory_reports,error=error),status
