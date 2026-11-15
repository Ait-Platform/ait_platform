"""Mechanic adapter for the established quotation register and item-edit pattern."""
from decimal import Decimal,InvalidOperation,ROUND_HALF_UP,localcontext
from hashlib import sha256
import json
from datetime import timezone
from zoneinfo import ZoneInfo
from flask import abort,request,redirect,url_for,render_template,flash
from flask_login import current_user
from app.extensions import db
from app.models.mechanic import MechShop,MechClient,MechVehicle,MechJobCard,MechPartLine,MechLaborLine,MechInvoice
from app.models.debtors import Debtor,BusinessBankAccount


def totals(job):
    with localcontext() as context:
        context.prec=50
        parts=[dict(kind='part',item=line,quantity=str(line.quantity or 0),rate=str(line.markup_price or 0),amount=Decimal(str(line.quantity or 0))*Decimal(str(line.markup_price or 0))) for line in job.part_lines]
        labour=[dict(kind='labour',item=line,quantity=str(line.hours or 0),rate=str(line.rate_per_hour or 0),amount=Decimal(str(line.hours or 0))*Decimal(str(line.rate_per_hour or 0))) for line in job.labor_lines]
        subtotal=sum((line['amount'] for line in parts+labour),Decimal('0'))
        if job.tax_snapshot:
            from app.protrade_vat import refresh_snapshot
            tax=refresh_snapshot(job.tax_snapshot,subtotal)
            return dict(lines=parts+labour,subtotal=Decimal(tax['subtotal']),vat=Decimal(tax['vat']),total=Decimal(tax['total']))
        tax=subtotal*Decimal(str(job.vat_rate or 0))/100
        return dict(lines=parts+labour,subtotal=subtotal,vat=tax,total=(subtotal+tax).quantize(Decimal('.01'),rounding=ROUND_HALF_UP))


def register_page(shop,debtor):
    if request.values.get('area')=='new':
        return redirect(url_for('mechanic_bp.new_quote',shop_id=shop.id,debtor_id=debtor.id,client_id=debtor.reference_id))
    jobs=MechJobCard.query.join(MechVehicle).join(MechClient).filter(MechJobCard.shop_id==shop.id,MechJobCard.debtor_id==debtor.id,MechClient.id==debtor.reference_id,MechClient.user_id==current_user.id).order_by(MechJobCard.created_at.desc(),MechJobCard.id.desc()).all()
    dates={job.id:(job.created_at if job.created_at.tzinfo else job.created_at.replace(tzinfo=timezone.utc)).astimezone(ZoneInfo('Africa/Johannesburg')).strftime('%Y-%m-%d') for job in jobs if job.created_at}
    return render_template('program_mechanic/mechanic_quote_register.html',shop=shop,debtor=debtor,jobs=jobs,amounts={job.id:totals(job)['total'] for job in jobs},dates=dates)


def items_page(shop_id,debtor_id,quote_id):
    shop=MechShop.query.filter_by(id=shop_id,user_id=current_user.id).first()
    debtor=Debtor.query.filter_by(id=debtor_id,shop_id=shop_id,user_id=current_user.id).first()
    if not shop or (shop.trade or 'mechanic')!='mechanic' or not debtor:abort(404)
    query=MechJobCard.query.join(MechVehicle).join(MechClient).filter(MechJobCard.id==quote_id,MechJobCard.shop_id==shop_id,MechJobCard.debtor_id==debtor_id,MechClient.id==debtor.reference_id,MechClient.user_id==current_user.id)
    job=(query.populate_existing().with_for_update(of=MechJobCard) if request.method=='POST' else query).first()
    if job is None:abort(404)
    kind=request.values.get('kind');identifier=request.values.get('item_id',type=int)
    line=None
    if kind is not None or request.values.get('item_id') is not None:
        if kind not in ('part','labour') or identifier is None:abort(404)
        model=MechPartLine if kind=='part' else MechLaborLine
        line=model.query.filter_by(id=identifier,job_card_id=job.id).first()
        if line is None:abort(404)
    url=url_for('mechanic_bp.mechanic_quote_items',shop_id=shop.id,debtor_id=debtor.id,quote_id=job.id)
    snapshot=dict(description=line.description or '',quantity=str(line.quantity if kind=='part' else line.hours),rate=str(line.markup_price if kind=='part' else line.rate_per_hour)) if line else {}
    token=sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest()
    values=dict(snapshot);error=None;status=200
    if request.method=='POST':
        if MechInvoice.query.filter_by(job_card_id=job.id).filter(MechInvoice.source_data.isnot(None)).first():
            abort(409,description='This quotation has been invoiced. Create a new quotation for changes.')
        if line is None:abort(400)
        if job.status!='Quote':abort(409,description='Only quotations can be edited. This job has progressed.')
        try:
            if request.form.get('line_token')!=token:
                status=409;raise ValueError('This item has changed. Cancel and reopen before saving.')
            values={field:request.form.get(field,'').strip() for field in snapshot}
            quantity=Decimal(values['quantity']);rate=Decimal(values['rate'])
            if not values['description'] or len(values['description'])>255:raise ValueError('Enter a description up to 255 characters.')
            if not quantity.is_finite() or quantity<=0 or quantity>Decimal('999999') or quantity!=quantity.quantize(Decimal('.01')) or (kind=='part' and quantity!=quantity.to_integral_value()):
                raise ValueError('Enter a positive whole quantity for parts, or hours with up to two decimals for labour.')
            if not rate.is_finite() or rate<0 or rate>Decimal('999999999.99') or rate!=rate.quantize(Decimal('.01')):
                raise ValueError('Enter a non-negative rate with up to two decimals.')
            line.description=values['description']
            if kind=='part':line.quantity=int(quantity);line.markup_price=float(rate)
            else:line.hours=float(quantity);line.rate_per_hour=float(rate)
            if job.tax_snapshot:
                from app.protrade_vat import refresh_snapshot
                raw=sum((Decimal(str(p.quantity or 0))*Decimal(str(p.markup_price or 0)) for p in job.part_lines),Decimal(0))
                raw+=sum((Decimal(str(l.hours or 0))*Decimal(str(l.rate_per_hour or 0)) for l in job.labor_lines),Decimal(0))
                job.tax_snapshot=refresh_snapshot(job.tax_snapshot,raw)
            db.session.commit()
            flash('Quote item updated.','success')
            return redirect(url)
        except (ValueError,InvalidOperation) as exc:
            error=str(exc) if isinstance(exc,ValueError) else 'Enter valid numeric values.'
            status=409 if status==409 else 400
    bank=BusinessBankAccount.query.filter_by(user_id=current_user.id).order_by(BusinessBankAccount.is_default.desc()).first()
    return render_template('program_mechanic/mechanic_quote_items.html',shop=shop,debtor=debtor,job=job,line=line,kind=kind,values=values,line_token=token,error=error,register_url=url,bank_account=bank,**totals(job)),status
