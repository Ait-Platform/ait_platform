"""Explicit paid SOA generation; saved snapshots and exports are immutable/free."""
import hashlib,json
from datetime import date,datetime
from zoneinfo import ZoneInfo
from types import SimpleNamespace
from io import BytesIO
from flask import abort,request,redirect,url_for,render_template,send_file
from flask_login import current_user
from app.extensions import db
from app.models.debtors import Debtor,DebtorLedger,BusinessBankAccount,SenderProfile
from app.models.mechanic import MechShop,ProTradeDocument
from app.protrade_ledger import ledger_period,format_cents
from app.protrade_documents import charge_document
from app.utils.pdf_render import html_to_pdf_bytes


def owned_statement(identifier,debtor_id=None,shop_id=None):
    query=ProTradeDocument.query.filter_by(id=identifier,user_id=current_user.id,kind='soa')
    if debtor_id is not None:query=query.filter_by(debtor_id=debtor_id)
    if shop_id is not None:query=query.filter_by(shop_id=shop_id)
    document=query.first()
    if not document:abort(404)
    return document


def statement_state(shop,debtor,start,end):
    rows=DebtorLedger.query.filter_by(debtor_id=debtor.id).populate_existing().order_by(DebtorLedger.txn_date,DebtorLedger.id).all()
    visible,opening,closing=ledger_period(rows,start,end)
    bank=BusinessBankAccount.query.filter_by(id=debtor.bank_account_id,user_id=current_user.id).first() if debtor.bank_account_id else None
    if debtor.bank_account_id and bank is None:abort(404)
    issuer={key:getattr(shop,key,None) for key in ('id','business_name','address','phone','email','logo_url','letterhead_url','use_custom_letterhead','bank_details','terms_and_conditions','tax_number')}
    issuer['trade'] = shop.trade or 'mechanic'
    if issuer['trade'] == 'mechanic':
        profile_query = SenderProfile.query.filter_by(user_id=current_user.id)
        profile_id = getattr(debtor, 'sender_profile_id', None)
        profile = profile_query.filter_by(id=profile_id).first() if profile_id else None
        if profile_id and profile is None:
            abort(404)
        if profile:
            for key in ('business_name','address','phone','email','logo_url','letterhead_url','use_custom_letterhead'):
                issuer[key] = getattr(profile, key, None)
    recipient={key:getattr(debtor,key,None) for key in ('id','name','phone','email','address')}
    bank_data={key:getattr(bank,key,None) for key in ('bank_name','account_name','account_number','bsb_branch','swift_code','raw_details')} if bank else None
    transactions=[dict(id=row.id,txn_date=row.txn_date.isoformat(),description=row.description,ref=row.ref,kind=row.kind,amount=int(row.amount),payment_method=row.payment_method,running_balance=int(row.running_balance)) for row in visible]
    return dict(issuer=issuer,debtor=recipient,bank=bank_data,start=start.isoformat() if start else None,end=end.isoformat() if end else None,opening=int(opening),closing=int(closing),transactions=transactions)


def snapshot_context(snapshot):
    rows=[SimpleNamespace(**dict(row,txn_date=date.fromisoformat(row['txn_date']))) for row in snapshot['transactions']]
    recipient=SimpleNamespace(**dict(snapshot['debtor'],created_at=datetime.fromisoformat(snapshot['statement_date'])))
    profile=SimpleNamespace(**snapshot['issuer'])
    return dict(carpentry_statement=snapshot['issuer'].get('trade', 'carpentry')!='mechanic',shop=profile,carpentry_money=format_cents,
        debtor=recipient,ledgers=rows,profile=profile,running_balance=snapshot['closing'],period_opening_balance=snapshot['opening'],
        latest_job_card=None,start_date=date.fromisoformat(snapshot['start']) if snapshot['start'] else None,
        end_date=date.fromisoformat(snapshot['end']) if snapshot['end'] else None,
        bank_account=SimpleNamespace(**snapshot['bank']) if snapshot['bank'] else None,statement_date=snapshot['statement_date'])


def snapshot_pdf(snapshot):
    html=render_template('program_debtors/soa_template.html',**snapshot_context(snapshot),return_url=None,is_pdf=True)
    pdf=html_to_pdf_bytes(html,base_url=request.host_url)
    if not pdf.startswith(b'%PDF-'):raise ValueError('No PDF generated')
    return pdf


def statement_page(debtor):
    shop=MechShop.query.filter_by(id=debtor.shop_id,user_id=current_user.id).first()
    if not shop:abort(404)
    values=request.form if request.method=='POST' else request.args
    try:
        start=date.fromisoformat(values['start_date']) if values.get('start_date') else None
        end=date.fromisoformat(values['end_date']) if values.get('end_date') else None
        if start and end and start>end:raise ValueError()
    except ValueError:abort(400,description='Choose a valid reporting period.')
    identifier=request.args.get('document_id',type=int)
    document=owned_statement(identifier,debtor.id,shop.id) if identifier else None
    state=statement_state(shop,debtor,start,end)
    key='soa:'+str(debtor.id)+':'+hashlib.sha256(json.dumps(state,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    unchanged=ProTradeDocument.query.filter_by(user_id=current_user.id,document_key=key).first()
    document=document or unchanged
    if request.args.get('pdf')=='1':
        if not document:abort(409,description='Generate and save the statement first. Opening this page did not charge tokens.')
        return send_file(BytesIO(document.pdf),mimetype='application/pdf',as_attachment=request.args.get('download')=='1',download_name=f'SOA_Shop-{shop.id}_Debtor-{debtor.id}_{document.id}.pdf')
    if request.method=='POST':
        if values.get('action')!='generate':abort(400)
        if unchanged:document=unchanged
        else:
            state['statement_date']=datetime.now(ZoneInfo('Africa/Johannesburg')).date().isoformat()
            try:pdf=snapshot_pdf(state)
            except Exception:
                db.session.rollback();abort(503,description='The statement PDF could not be generated. No tokens were charged. Please try again.')
            document=charge_document(current_user.id,shop.id,debtor.id,key,'soa',snapshot=state,pdf=pdf)
            db.session.commit()
        return redirect(url_for('debtors_bp.generate_soa',debtor_id=debtor.id,document_id=document.id))
    if document and request.args.get('generate')!='1':
        return render_template('program_debtors/soa_template.html',**snapshot_context(document.snapshot),
            generated_document=document,return_url=url_for('mechanic_bp.client_ledger',debtor_id=debtor.id,shop_id=shop.id),is_pdf=False)
    history=ProTradeDocument.query.filter_by(user_id=current_user.id,shop_id=shop.id,debtor_id=debtor.id,kind='soa').order_by(ProTradeDocument.id.desc()).all()
    return render_template('program_mechanic/debtor_soa.html',shop=shop,debtor=debtor,document=document,history=history,start_date=start,end_date=end,money=format_cents,preview=state)


def output_snapshot(document):
    snapshot=document.snapshot;issuer=dict(snapshot['issuer']);issuer['name']=issuer['business_name']
    if snapshot['bank']:
        issuer['bank_details']=snapshot['bank'].get('raw_details') or ' / '.join(str(snapshot['bank'].get(key) or '') for key in ('bank_name','account_name','account_number','bsb_branch','swift_code'))
    rows=[['','Opening balance','','',format_cents(snapshot['opening'])]]
    for row in snapshot['transactions']:
        rows.append([row['txn_date'],row['description']+(' - '+row['payment_method'] if row.get('payment_method') else '')+((' / '+row['ref']) if row.get('ref') else ''),format_cents(row['amount']) if row['kind']=='debit' else '',format_cents(row['amount']) if row['kind']=='credit' else '',format_cents(row['running_balance'])])
    return dict(issuer=issuer,document=dict(kind='statement',title='Statement of Account',reference='SOA-'+str(document.id),date=snapshot['statement_date'],recipient=snapshot['debtor'],
        columns=['Date','Details','Debit (R)','Credit (R)','Balance (R)'],rows=rows,totals=[('Amount Due (R)',format_cents(snapshot['closing']))],
        note='Reporting period: '+(snapshot['start'] or 'Start')+' to '+(snapshot['end'] or 'All recorded transactions')),stored_soa_id=document.id)
