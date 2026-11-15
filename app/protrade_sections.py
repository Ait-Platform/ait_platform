"""BOQ categories reuse quotation lines and immutable wallet receipts."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from io import BytesIO
from uuid import uuid4
from flask import abort, request, render_template, redirect, url_for, send_file
from flask_login import current_user
from app.extensions import db
from app.models.mechanic import MechShop, ProTradeBoqQuote, ProTradeDocument
from app.protrade_documents import charge_document


BOQ_REVENUE_TERMS = {
    'currency': 'ZAR',
    'contractor_fee_cents': 5000,
    'operator_retains_cents': 2500,
    'ait_wallet_value_cents': 2500,
    'tokens': 25,
    'contractor_payment_recipient': 'operator',
    'ait_collection': 'prepaid_wallet_deduction',
}


def receipt_key(quote, section):
    return f"boq-section:{quote.id}:{section['id']}"


def paid_receipt(quote, section):
    return ProTradeDocument.query.filter_by(user_id=quote.user_id, shop_id=quote.shop_id,
        document_key=receipt_key(quote, section), kind='boq_section').first()


def section_lines(quote, section):
    lines = [line for line in quote.items if str(line['source_item_id']) in section['items']]
    if len(lines) != len(section['items']):
        abort(409, description='A section item has been removed. Restore its item before generating this section.')
    return lines


def require_paid_sections(quote):
    assigned = []
    for section in quote.boq_sections or []:
        section_lines(quote, section)
        if not paid_receipt(quote, section):
            abort(402, description='Generate each BOQ section before downloading the complete BOQ.')
        assigned.extend(section['items'])
    if sorted(assigned) != sorted(str(line['source_item_id']) for line in quote.items) or not assigned:
        abort(409, description='Assign every BOQ item to a section before downloading the complete BOQ.')


def section_data(shop, quote, section):
    from app.protrade_output import report_data
    data = report_data(shop, dict(kind='quote', key=f'quote:boq-{quote.id}', label=quote.display_quote_number, source=quote))
    lines = section_lines(quote, section)
    data['document'].update(title='BOQ Section', reference=quote.display_quote_number + ' - ' + section['name'],
        rows=[[line['code'], line['description'], line['unit'], line['quantity'], line['material_rate'], line['labour_rate'], line['total']] for line in lines],
        totals=[('Total (R)', format(sum(Decimal(line['total']) for line in lines), '.2f'))])
    return data


def sections_page(shop_id, quote_id):
    shop = MechShop.query.filter_by(id=shop_id, user_id=current_user.id, trade='boq').first_or_404()
    query = ProTradeBoqQuote.query.filter_by(id=quote_id, shop_id=shop.id, user_id=current_user.id, trade='boq')
    if request.method == 'POST':
        query = query.populate_existing().with_for_update()
    quote = query.first_or_404()
    sections = deepcopy(quote.boq_sections or [])
    section = next((item for item in sections if item['id'] == request.values.get('section_id')), None)
    action = request.form.get('action')
    if request.method == 'POST':
        if action == 'save':
            name = request.form.get('name', '').strip()
            identifiers = request.form.getlist('item_id')
            available = {str(line['source_item_id']) for line in quote.items}
            used = {identifier for item in sections for identifier in item['items']}
            if not name or len(name) > 120 or not identifiers or len(set(identifiers)) != len(identifiers):
                abort(400, description='Enter a section name and select its work items.')
            if any(item['name'].casefold() == name.casefold() for item in sections):
                abort(409, description='This section name already exists.')
            if not set(identifiers) <= available - used:
                abort(409, description='Select unassigned items from this BOQ.')
            sections.append(dict(id=uuid4().hex, name=name, items=identifiers))
            quote.boq_sections = sections
            db.session.commit()
        elif action == 'generate' and section:
            if paid_receipt(quote, section):
                return redirect(url_for('mechanic_bp.boq_sections', shop_id=shop.id, quote_id=quote.id))
            # Render before charging so PDF failures roll back without a deduction.
            from app.protrade_output import pdf_bytes
            data = section_data(shop, quote, section)
            try:
                pdf = pdf_bytes(data, dict(generated=datetime.now(timezone.utc).isoformat()))
            except Exception:
                db.session.rollback()
                abort(503, description='Section PDF generation failed.')
            if not pdf.startswith(b'%PDF-'):
                abort(503, description='Section PDF generation failed.')
            charge_document(current_user.id, shop.id, quote.debtor_id, receipt_key(quote, section),
                'boq_section', snapshot=dict(section=section, document=data, commercial_terms=dict(BOQ_REVENUE_TERMS)), pdf=pdf)
            db.session.commit()
        else:
            abort(400)
        return redirect(url_for('mechanic_bp.boq_sections', shop_id=shop.id, quote_id=quote.id))
    if request.args.get('download') == '1':
        if not section or not paid_receipt(quote, section):
            abort(402, description='Generate this section before downloading.')
        from app.protrade_output import pdf_bytes
        pdf = pdf_bytes(section_data(shop, quote, section), dict(generated=datetime.now(timezone.utc).isoformat()))
        return send_file(BytesIO(pdf), mimetype='application/pdf', as_attachment=True, download_name='BOQ-section.pdf')
    used = {identifier for item in sections for identifier in item['items']}
    available = [line for line in quote.items if str(line['source_item_id']) not in used]
    paid = {item['id'] for item in sections if paid_receipt(quote, item)}
    return render_template('program_mechanic/boq_sections.html', shop=shop, quote=quote, sections=sections, available=available, paid=paid, revenue_terms=BOQ_REVENUE_TERMS)
