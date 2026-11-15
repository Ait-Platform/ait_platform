"""Effective-dated VAT, decimal arithmetic and explicit, immutable invoice conversion."""
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from zoneinfo import ZoneInfo
import re
from flask import abort, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from app.extensions import db
from app.models.mechanic import MechShop, MechJobCard, MechInvoice, ProTradeBoqQuote, ProTradeVatRate
from app.models.debtors import Debtor
from app.models.auth import User
from app.utils.roles import is_admin


def money(value):
    amount=Decimal(str(value))
    if not amount.is_finite():raise ValueError('Enter a finite monetary amount.')
    with localcontext() as context:
        context.prec=50
        return amount.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)


def today():
    return datetime.now(ZoneInfo('Africa/Johannesburg')).date()


def rate_for(day):
    row=ProTradeVatRate.query.filter(ProTradeVatRate.effective_date <= day).order_by(ProTradeVatRate.effective_date.desc(),ProTradeVatRate.id.desc()).first()
    if row is None:
        raise ValueError('No approved VAT rate for this supply date. Contact AIT Admin.')
    return row


def vat_number(value):
    return bool(re.fullmatch(r'4\d{9}', (value or '').strip()))


def calculate(amount, shop, treatment='standard', inclusive=False, supply_date=None, basis=''):
    if shop.vat_registered is None:
        raise ValueError('Confirm VAT registration in this contractor’s Shop Profile first.')
    if treatment not in ('standard','zero','exempt'):
        raise ValueError('Select standard-rated, legally zero-rated or exempt treatment.')
    if shop.vat_registered and not vat_number(shop.tax_number):
        raise ValueError('The VAT-registered shop needs a valid 10-digit VAT registration number starting with 4.')
    day=supply_date or today()
    rate=Decimal(0);rate_id=None
    if shop.vat_registered:
        if treatment=='standard':
            approved=rate_for(day);rate=approved.rate;rate_id=approved.id
            basis=''
        elif not 8 <= len(basis.strip()) <= 400:
            raise ValueError('Record the legal basis for a zero-rated or exempt supply (8–400 characters).')
    else:
        treatment='nonvendor';basis='Supplier is not VAT registered'
    amount=money(amount)
    if amount<0:raise ValueError('Quotation totals cannot be negative.')
    with localcontext() as context:
        context.prec=50
        tax=money(amount*rate/(100+rate) if inclusive else amount*rate/100)
        net=amount-tax if inclusive else amount
        total=net+tax
    if total>Decimal('99999999999999999999999999.99'):raise ValueError('Quotation total is too large to save.')
    return dict(registered=bool(shop.vat_registered),vat_number=shop.tax_number if shop.vat_registered else '',
        treatment=treatment,legal_basis=basis.strip(),inclusive=bool(inclusive),rate=str(rate),rate_id=rate_id,
        supply_date=day.isoformat(),subtotal=str(net),vat=str(tax),total=str(total))


def from_form(amount,shop):
    try:
        day=date.fromisoformat(request.form.get('supply_date') or today().isoformat())
    except ValueError:
        raise ValueError('Enter a valid supply date.') from None
    return calculate(amount,shop,request.form.get('vat_treatment','standard'),
        request.form.get('vat_inclusive')=='yes',day,request.form.get('vat_basis',''))


def refresh_snapshot(snapshot,amount):
    """An edit retains the quotation's declared supply treatment and approved rate."""
    tax=deepcopy(snapshot);amount=money(amount);rate=Decimal(tax['rate'])
    with localcontext() as context:
        context.prec=50
        vat=money(amount*rate/(100+rate) if tax['inclusive'] else amount*rate/100)
        net=amount-vat if tax['inclusive'] else amount
        tax.update(subtotal=str(net),vat=str(vat),total=str(net+vat))
    return tax


def invoice_output(job):
    invoice=MechInvoice.query.filter_by(job_card_id=job.id).filter(MechInvoice.source_data.isnot(None)).first()
    if invoice:
        return redirect(url_for('mechanic_bp.protrade_output',shop_id=job.shop_id,selection=f'invoice:converted-{invoice.id}',preview=1))


def vat_settings():
    if not is_admin(current_user):abort(403)
    error=None;status=200
    if request.method=='POST':
        User.query.filter_by(id=current_user.id).with_for_update().first()
        try:
            day=date.fromisoformat(request.form.get('effective_date',''))
            rate=Decimal(request.form.get('rate',''))
            reason=request.form.get('reason','').strip()
            if day<today():raise ValueError('New VAT settings cannot change past effective dates.')
            if not rate.is_finite() or not 0<rate<=100 or rate!=rate.quantize(Decimal('.01')):raise ValueError('Enter a positive percentage with at most two decimal places.')
            if not 8<=len(reason)<=400:raise ValueError('Record the statutory source and approval reason (8–400 characters).')
            db.session.add(ProTradeVatRate(effective_date=day,rate=rate,user_id=current_user.id,reason=reason))
            db.session.commit();flash('Approved VAT rate applies by supply date; saved documents are unchanged.','success')
            return redirect(url_for('mechanic_bp.protrade_vat_settings'))
        except (ValueError,InvalidOperation) as exc:
            db.session.rollback();error,status=str(exc),400
    rows=ProTradeVatRate.query.order_by(ProTradeVatRate.effective_date.desc(),ProTradeVatRate.id.desc()).all()
    return render_template('program_mechanic/vat_settings.html',rows=rows,today=today().isoformat(),error=error),status


def conversion(shop_id,kind,quote_id):
    shop=MechShop.query.filter_by(id=shop_id,user_id=current_user.id).first()
    if not shop or kind not in ('boq','mechanic'):abort(404)
    model=ProTradeBoqQuote if kind=='boq' else MechJobCard
    query=model.query.filter_by(id=quote_id,shop_id=shop.id)
    quote=(query.populate_existing().with_for_update() if request.method=='POST' else query).first()
    if not quote:abort(404)
    if kind=='boq' and (quote.user_id!=current_user.id or quote.trade!=shop.trade):abort(404)
    if kind=='mechanic' and (shop.trade not in ('mechanic',None,'') or quote.vehicle.client.user_id!=current_user.id):abort(404)
    debtor=Debtor.query.filter_by(id=quote.debtor_id,shop_id=shop.id,user_id=current_user.id).first()
    if not debtor:abort(404)
    source_field='boq_quote_id' if kind=='boq' else 'job_card_id'
    existing=MechInvoice.query.filter_by(**{source_field:quote.id}).order_by(MechInvoice.id).first()
    if existing:
        return redirect(url_for('mechanic_bp.protrade_output',shop_id=shop.id,
            selection=f'invoice:converted-{existing.id}' if existing.source_data else f'invoice:stored-{existing.id}',preview=1))
    error=None;status=200
    if request.method=='POST':
        try:
            if request.form.get('confirm_invoice')!='yes':raise ValueError('Confirm issuing a separate invoice from this quotation.')
            if not all((shop.business_name,shop.address,debtor.name,debtor.address)):
                raise ValueError('Save supplier and customer names and addresses before issuing an invoice.')
            # Preserve the agreed gross total and the existing quotation ledger debit.
            if kind=='boq':gross=quote.grand_total
            else:
                from app.protrade_mechanic_quotes import totals
                gross=totals(quote)['total']
            day=date.fromisoformat(request.form.get('supply_date',''))
            if day>today():raise ValueError('The actual invoice supply date cannot be in the future.')
            tax=calculate(gross,shop,request.form.get('vat_treatment','standard'),True,day,request.form.get('vat_basis',''))
            recipient_vendor=request.form.get('recipient_vendor')=='yes'
            recipient_vat=request.form.get('recipient_vat','').strip()
            if recipient_vendor and not vat_number(recipient_vat):raise ValueError('Enter the recipient vendor’s VAT registration number.')
            if shop.trade=='boq':
                from app.protrade_sections import require_paid_sections
                require_paid_sections(quote)
            from app.protrade_output import report_data
            entry=dict(kind='quote',key=f'quote:boq-{quote.id}' if kind=='boq' else f'quote:job-{quote.id}',label=quote.display_quote_number,source=quote)
            bundle=deepcopy(report_data(shop,entry))
            document=bundle['document']
            if not document['rows']:raise ValueError('Add work items to the quotation before converting it.')
            # Show inclusive line prices against the unchanged agreed gross amount.
            # This also handles a different statutory rate at actual time of supply.
            raw=sum((Decimal(row[-1]) for row in document['rows']),Decimal(0))
            allocated=Decimal(0)
            for index,row in enumerate(document['rows']):
                value=money(Decimal(row[-1])*Decimal(tax['total'])/raw) if raw else Decimal(0)
                if index==len(document['rows'])-1:value=Decimal(tax['total'])-allocated
                allocated+=value
                for column in ([4,5] if len(row)==7 else [2]):
                    row[column]=str(money(Decimal(row[column])*Decimal(tax['total'])/raw)) if raw else '0.00'
                row[-1]=str(value)
            if tax['registered'] and tax['treatment'] in ('standard','zero'):
                document['columns']=[label+' (VAT incl.)' if position in ([4,5,6] if len(document['columns'])==7 else [2,3]) else label for position,label in enumerate(document['columns'])]
            document.update(title='Tax Invoice' if tax['registered'] and tax['treatment'] in ('standard','zero') else 'Invoice',
                date=today().isoformat(),tax=tax,totals=[('Subtotal (R)',tax['subtotal']),(f"VAT ({tax['rate']}%) (R)",tax['vat']),('Total (R)',tax['total'])],
                note=f"Source quotation {quote.display_quote_number}. Supply date {day.isoformat()}. {tax['legal_basis']}")
            document['recipient'].update(name=debtor.name,address=debtor.address,vat_number=recipient_vat if recipient_vendor else '')
            bundle['issuer'].update(vat_registered=tax['registered'],tax_number=tax['vat_number'])
            invoice=MechInvoice(**{source_field:quote.id},subtotal=Decimal(tax['subtotal']),tax=Decimal(tax['vat']),total=Decimal(tax['total']))
            db.session.add(invoice);db.session.flush()
            invoice.invoice_number=f'INV-{invoice.id:06d}';document['reference']=invoice.invoice_number
            invoice.source_data=bundle
            from app.protrade_documents import charge_document
            key='mechanic-invoice:'+str(quote.id) if kind=='mechanic' else 'boq-invoice:'+str(quote.id)
            charge_document(current_user.id,shop.id,debtor.id,key,'invoice',snapshot=dict(invoice=invoice.invoice_number,source_quote=quote.display_quote_number))
            from app.protrade_boq_time import complete_quote
            if kind=='boq':complete_quote(quote)
            db.session.commit()
            return redirect(url_for('mechanic_bp.protrade_output',shop_id=shop.id,selection=f'invoice:converted-{invoice.id}',preview=1))
        except (ValueError,InvalidOperation) as exc:
            db.session.rollback();error,status=str(exc),400
    return render_template('program_mechanic/convert_invoice.html',shop=shop,debtor=debtor,quote=quote,kind=kind,today=today().isoformat(),error=error),status


def register_routes(bp):
    bp.add_url_rule('/mechanic/admin/vat','protrade_vat_settings',login_required(vat_settings),methods=['GET','POST'])
    bp.add_url_rule('/mechanic/shop/<int:shop_id>/<kind>/<int:quote_id>/invoice','convert_invoice',login_required(conversion),methods=['GET','POST'])
    def context():
        try:rate=rate_for(today()).rate
        except ValueError:rate=None
        schedule=[dict(day=row.effective_date.isoformat(),rate=str(row.rate)) for row in ProTradeVatRate.query.order_by(ProTradeVatRate.effective_date,ProTradeVatRate.id).all()]
        return dict(protrade_vat_rate=rate,protrade_today=today().isoformat(),protrade_vat_schedule=schedule)
    bp.context_processor(context)
