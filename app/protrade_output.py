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
                                ProTradeBoqQuote,ProTradeOutputEmail,ProTradeDocument)
from app.models.debtors import Debtor,DebtorLedger

LABELS={'quote':'Quotations','invoice':'Invoices','statement':'Debtor Statements','price_list':'Price Lists'}


def folder_reports(shops, short_codes=False):
    """Browser destinations reuse owned shops; legacy source/title paths stay readable."""
    import re
    def label(value,fallback):
        value=re.sub(r'[<>:"/\\|?*\x00-\x1f]','_',str(value or '')).strip(' .')[:100]
        if not value or re.fullmatch(r'(?i)(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?',value):return fallback
        return value
    user=label(current_user.name,'User-'+str(current_user.id))
    reports=[]
    for shop in shops:
        for kind,title in dict(LABELS,**({'boq_section':'BOQ Sections'} if shop.trade=='boq' else {})).items():
            group='SOA' if kind=='statement' else 'BOQ' if kind in ('price_list','boq_section') or (kind=='quote' and shop.trade=='boq') else 'Quotes & Invoices'
            reports.append(dict(code=kind if short_codes else f'{shop.id}:{kind}',title=title,
                source=secure_filename(f'Shop-{shop.id}-{shop.business_name}')[:80] or f'Shop-{shop.id}',
                path=[user,label(shop.business_name,'Shop-'+str(shop.id)),group],
                owner=dict(program='protrade',user=str(current_user.id),client=str(shop.id))))
    return reports


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
    if (shop.trade or 'mechanic')!='mechanic':
        for quote in ProTradeBoqQuote.query.filter_by(shop_id=shop.id,user_id=current_user.id,trade=shop.trade).order_by(ProTradeBoqQuote.id.desc()).all():
            if quote.debtor_id is not None:
                debtor=Debtor.query.filter_by(id=quote.debtor_id,shop_id=shop.id,user_id=current_user.id).first()
                if not debtor or debtor.reference_id!=quote.client_id:continue
            if quote.client_id is not None:
                client=MechClient.query.filter_by(id=quote.client_id,user_id=current_user.id).first()
                if not client or client.shop_id not in (None,shop.id):continue
            entries.append(dict(key=f'quote:boq-{quote.id}',kind='quote',label=quote.display_quote_number+' — '+quote.client_name,source=quote))
        if ProTradeBoqItem.query.filter_by(shop_id=shop.id,user_id=current_user.id,trade=shop.trade).first():
            entries.append(dict(key='price_list:all',kind='price_list',label=('Carpentry BOQ Price List' if shop.trade=='carpentry' else 'BOQ Price List'),source=None))
    else:
        if (shop.trade or 'mechanic')=='mechanic':
            jobs=MechJobCard.query.join(MechVehicle).join(MechClient).filter(MechJobCard.shop_id==shop.id,MechClient.user_id==current_user.id).order_by(MechJobCard.id.desc()).all()
            for job in jobs:
                invoices=MechInvoice.query.filter_by(job_card_id=job.id).order_by(MechInvoice.id).all()
                if job.status!='Billed' or job.tax_snapshot:
                    entries.append(dict(key=f'quote:job-{job.id}',kind='quote',label=job.job_number+' — '+job.vehicle.client.name,source=job))
                elif not invoices:
                    entries.append(dict(key=f'invoice:billed-{job.id}',kind='invoice',label=job.job_number+' — Existing billed document',source=job))
                for invoice in invoices:
                    if invoice.source_data:continue
                    entries.append(dict(key=f'invoice:stored-{invoice.id}',kind='invoice',label=job.job_number+f' — Invoice record {invoice.id}',source=(invoice,job)))
        if price_items(shop):entries.append(dict(key='price_list:all',kind='price_list',label='Price List',source=None))
    from sqlalchemy import or_
    quote_ids=ProTradeBoqQuote.query.filter_by(shop_id=shop.id,user_id=current_user.id).with_entities(ProTradeBoqQuote.id)
    job_ids=MechJobCard.query.filter_by(shop_id=shop.id).with_entities(MechJobCard.id)
    for invoice in MechInvoice.query.filter(MechInvoice.source_data.isnot(None),or_(MechInvoice.boq_quote_id.in_(quote_ids),MechInvoice.job_card_id.in_(job_ids))).all():
        if invoice.job_card_id:
            job=MechJobCard.query.filter_by(id=invoice.job_card_id,shop_id=shop.id).first()
            if not job or not job_debtor(shop,job):continue
        entries.append(dict(key=f'invoice:converted-{invoice.id}',kind='invoice',label=invoice.invoice_number,source=invoice))
    for document in ProTradeDocument.query.filter_by(user_id=current_user.id,shop_id=shop.id,kind='soa').order_by(ProTradeDocument.id.desc()).all():
        entries.append(dict(key=f'statement:generated-{document.id}',kind='statement',label='SOA-'+str(document.id)+' - '+document.snapshot['debtor']['name'],source=document))
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
    if entry['key'].startswith('invoice:converted-'):
        from copy import deepcopy
        bundle=deepcopy(source.source_data)
        if bundle['issuer']['id']!=shop.id:abort(404)
        debtor=owned_debtor(shop,bundle['email_recipient']['id'])
        if source.boq_quote_id:
            if not ProTradeBoqQuote.query.filter_by(id=source.boq_quote_id,shop_id=shop.id,user_id=current_user.id,debtor_id=debtor.id).first():abort(404)
        else:
            job=MechJobCard.query.filter_by(id=source.job_card_id,shop_id=shop.id,debtor_id=debtor.id).first()
            if not job or job_debtor(shop,job).id!=debtor.id:abort(404)
        if recipient_id and str(recipient_id)!=str(debtor.id):abort(400)
        bundle['email_recipient']=dict(id=debtor.id,**contact(debtor))
        return bundle
    data=dict(kind=kind,title=LABELS[kind].rstrip('s'),reference=entry['label'],recipient=None,
              columns=[],rows=[],totals=[],note='',date='')
    if entry['key'].startswith('quote:boq-'):
        if source.debtor_id is not None:
            debtor=owned_debtor(shop,source.debtor_id)
            if source.client_id!=debtor.reference_id:abort(404)
        data.update(reference=source.display_quote_number,date=document_date(source.created_at),
                    recipient=dict(name=source.client_name,phone=source.client_phone or '',email=source.client_email or '',address=source.client_address or ''),
                    columns=['BOQ Code','Description','Unit','Quantity','Material R/unit','Labour R/unit','Total R'],
                    rows=[[line['code'],line['description'],line['unit'],line['quantity'],line['material_rate'],line['labour_rate'],line['total']] for line in source.items],
                    totals=[('Total (R)',format(source.grand_total,'.2f'))])
    elif kind in ('quote','invoice'):
        invoice,job=source if isinstance(source,tuple) else (None,source)
        debtor=job_debtor(shop,job)
        data.update(reference=job.job_number if kind=='invoice' else job.display_quote_number,date=document_date(invoice.created_at if invoice else job.created_at),recipient=contact(job.vehicle.client))
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
    elif entry['key'].startswith('statement:generated-'):
        from app.protrade_statements import output_snapshot
        saved=source
        debtor=owned_debtor(shop,saved.debtor_id)
        data_bundle=output_snapshot(saved)
        data=data_bundle['document']
    elif kind=='statement':
        abort(409,description='Generate and save this debtor statement first using Generate SOA in the debtor ledger.')
    else:
        if shop.trade in ('carpentry','boq'):
            data.update(reference=('Carpentry-BOQ' if shop.trade=='carpentry' else 'BOQ'),columns=['BOQ Code','Description','Unit','Material R/unit','Labour R/unit','Unit Rate (R)'],
                        rows=[[item.code,item.description,item.unit,format(item.material_rate,'.2f'),format(item.labour_rate,'.2f'),format(item.total_rate,'.2f')]
                              for item in ProTradeBoqItem.query.filter_by(shop_id=shop.id,user_id=current_user.id,trade=shop.trade).order_by(ProTradeBoqItem.code).all()])
        else:
            data.update(reference='Price-List',columns=['Item','Category','Unit Price (R)'],rows=[[item.part_name,item.category or '',format(item.default_price or 0,'.2f')] for item in price_items(shop)])
        if recipient_id:debtor=owned_debtor(shop,recipient_id)
    if recipient_id and (not debtor or str(debtor.id)!=str(recipient_id)):
        abort(400,description='Select the saved debtor linked to this document.')
    email=dict(id=debtor.id,**contact(debtor)) if debtor else None
    issuer=dict(id=shop.id,name=shop.business_name or '',trade=shop.trade or 'mechanic',address=shop.address or '',phone=shop.phone or '',email=shop.email or '',tax_number=shop.tax_number or '')
    tax=getattr(source,'tax_snapshot',None) if kind=='quote' else None
    if tax:
        data['tax']=tax
        data['totals']=[('Subtotal (R)',tax['subtotal']),(f"VAT ({tax['rate']}%) (R)",tax['vat']),('Total (R)',tax['total'])]
        data['note']=tax['legal_basis']
        issuer.update(vat_registered=tax['registered'],tax_number=tax['vat_number'])
    if kind=='quote' and getattr(source,'trade',None)=='boq' and any(line.get('trade') for line in source.items):
        totals={}
        for line in source.items:
            trade=line.get('trade','boq');totals[trade]=totals.get(trade,Decimal(0))+Decimal(line['total'])
        data['totals']=[(trade.replace('_',' ').title()+' subtotal (entered rates)',format(amount,'.2f')) for trade,amount in sorted(totals.items())]+data['totals']
        if any(line.get('rate_basis')=='combined' for line in source.items):data['columns'][4]='Material / catalogue R/unit'
    if entry['key'].startswith('statement:generated-'):
        return dict(data_bundle,email_recipient=email)
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
    if data.get('stored_soa_id'):
        from app.protrade_statements import owned_statement
        return owned_statement(data['stored_soa_id'],shop_id=data['issuer']['id']).pdf
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
            if entry['kind']=='statement' and entry['key'].startswith('statement:debtor-') and args.get('format')!='pdf' and request.method=='GET':
                return redirect(url_for('debtors_bp.generate_soa',debtor_id=entry['source'].id))
            data=report_data(shop,entry,recipient_id)
            if request.method=='POST' or args.get('format')=='pdf':
                token=args.get('preview_token','');claim=verify_token(token,shop,selected,recipient_id,data)
            else:
                token=make_token(shop,selected,recipient_id,data);claim=signer().loads(token)
            data['filename']=f'{claim["generated"][:10]}_Shop-{shop.id}_{entry["kind"]}_{secure_filename(data["document"]["reference"])[:70]}_{claim["nonce"][:10]}.pdf'
            if request.method=='POST' and (args.get('action')!='email' or args.get('confirm_email')!='yes'):
                abort(400,description='Confirm sending to the saved debtor email address before sending.')
            if request.method=='POST' or args.get('format')=='pdf':
                if shop.trade=='boq' and entry['key'].startswith('quote:boq-'):
                    from app.protrade_sections import require_paid_sections
                    require_paid_sections(entry['source'])
                try:
                    pdf=pdf_bytes(data,claim)
                    if not pdf.startswith(b'%PDF-'):raise RuntimeError('PDF renderer did not return a PDF')
                except Exception:
                    current_app.logger.exception('ProTrade Output PDF failed')
                    abort(503,description='The PDF could not be generated. No report was saved or emailed. Contact the administrator to check the existing PDF renderer.')
                if request.method=='POST':
                    dispatch_once(shop,selected,data,claim,pdf)
                    if shop.trade=='boq' and entry['key'].startswith('quote:boq-'):
                        from app.protrade_boq_time import complete_quote
                        complete_quote(entry['source'])
                        db.session.commit()
                    flash('Mail transport accepted the document. Inbox delivery is not confirmed.','success')
                    return redirect(url_for('mechanic_bp.protrade_output',shop_id=shop.id))
                if shop.trade=='boq' and entry['key'].startswith('quote:boq-'):
                    from app.protrade_boq_time import complete_quote
                    complete_quote(entry['source'])
                    db.session.commit()
                return send_file(BytesIO(pdf),mimetype='application/pdf',as_attachment=True,download_name=data['filename'])
        except HTTPException as exc:
            if args.get('format')=='pdf':return jsonify(error=exc.description),exc.code
            error,status=exc.description,exc.code
            if claim is None:
                data=None;token=''
    pdf_url=url_for('mechanic_bp.protrade_output',shop_id=shop.id,selection=selected,recipient_id=recipient_id,format='pdf',preview_token=token) if data else ''
    from app.protrade_folders import local_reports
    local_directory_reports=local_reports(shops)
    local_save_code=str(shop.id)+':'+selected.split(':',1)[0] if shop and selected.split(':',1)[0] in ('quote','invoice','statement') else ''
    if data and local_save_code:
        for destination in local_directory_reports:
            if destination['code']==local_save_code:destination['title']=secure_filename(data['document']['reference']) or destination['title']
    return render_template('program_mechanic/output.html',local_directory_reports=local_directory_reports,local_save_code=local_save_code,shop=shop,shops=shops,choices=public_choices,labels=LABELS,selection=selected,
                           debtors=debtors,recipient_id=recipient_id,report_data=data,preview_token=token,generated=claim['generated'] if claim else '',
                           pdf_url=pdf_url,error=error),status


def support_page(page):
    """Read-only help; retired folder bookmarks retain ownership checks."""
    if page not in ('folders','help'):
        abort(404)
    shops=MechShop.query.filter_by(user_id=current_user.id).order_by(MechShop.business_name,MechShop.id).all()
    raw=request.args.get('shop_id',session.get('active_shop_id'))
    shop=None
    if raw is not None:
        try:identifier=int(raw)
        except (ValueError,TypeError):abort(404)
        shop=next((candidate for candidate in shops if candidate.id==identifier),None)
        if shop is None:abort(404)
    # Unique manager codes let the shared API create every shop's folders.
    # Output continues using its existing report codes and exactly the same paths.
    if page=='folders':
        return redirect(url_for('mechanic_bp.protrade_output',shop_id=shop.id if shop else None))
    directory_reports=folder_reports(shops)
    return render_template('program_mechanic/'+page+'.html',shop=shop,shops=shops,
                           directory_reports=directory_reports,protrade_menu_active=page)
