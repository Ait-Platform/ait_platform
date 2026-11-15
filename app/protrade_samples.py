"""Fictional marketing documents rendered with the existing ProTrade layout."""
from decimal import Decimal
from flask import abort, render_template, request

SAMPLE_DATE = '2026-11-15'
SAMPLES = [
    ('mechanic', 'General Mechanic', 'Example Auto Workshop', [('Vehicle diagnostic assessment','1','450'),('Service labour','2','350')]),
    ('carpentry', 'Carpentry', 'Example Carpentry Studio', [('Supply and fit timber door','1','1850'),('Install skirting per metre','12','130')]),
    ('plumbing', 'Plumbing', 'Example Plumbing Services', [('Replace kitchen mixer tap','1','950'),('Repair pipework per hour','2','375')]),
    ('electrical', 'Electrical', 'Example Electrical Services', [('Replace light fittings','4','280'),('Electrical fault assessment','1','550')]),
    ('building', 'Building Works', 'Example Building Services', [('Brickwork per square metre','10','420'),('Plaster repairs per square metre','8','165')]),
    ('painting', 'Painting', 'Example Painting Services', [('Prepare and paint walls per square metre','45','65'),('Paint internal doors','3','350')]),
    ('boq', 'Bill of Quantities', 'Example Quantity Surveying', [('Excavation per cubic metre','8','320'),('Concrete work per cubic metre','3','1650')]),
    ('crm_health', 'CRM Health - Medical Practice', 'Example Family Practice', [('Standard consultation','1','550'),('Follow-up appointment','1','340')]),
]


def sample_documents():
    documents = []
    for slug, label, business, items in SAMPLES:
        total = sum(Decimal(quantity) * Decimal(rate) for _, quantity, rate in items)
        recipient = dict(name='Example Patient' if slug=='crm_health' else 'Example Client',
            address='Fictional client address', phone='', email='client@example.test')
        issuer = dict(name=business, address='Johannesburg, South Africa', phone='',
            email='office@example.test', tax_number='', bank_details='')
        reference = 'SAMPLE-' + slug.upper().replace('_','-') + '-001'
        note = 'Fictional business and client. Illustrative amounts in ZAR.'
        quote = dict(kind='quote', title=label + ' - Sample Quotation', reference=reference,
            date=SAMPLE_DATE, recipient=recipient, note=note,
            columns=['Description','Quantity','Rate (R)','Amount (R)'],
            rows=[[description, quantity, format(Decimal(rate),',.2f'), format(Decimal(quantity)*Decimal(rate),',.2f')] for description,quantity,rate in items],
            totals=[('Quotation Total (R)',format(total,',.2f'))])
        payment = (total / 2).quantize(Decimal('0.01'))
        balance = total - payment
        statement = dict(kind='statement', title=label + ' - Sample Statement of Account',
            reference=reference+'-SOA', date=SAMPLE_DATE, recipient=recipient,
            note=note + ' Reporting period 2026-11-01 to 2026-11-15.',
            columns=['Date','Details','Debit (R)','Credit (R)','Balance (R)'],
            rows=[['2026-11-01','Opening balance','','','0.00'],
                  ['2026-11-03','Services supplied - '+reference,format(total,',.2f'),'',format(total,',.2f')],
                  ['2026-11-10','EFT payment received','',format(payment,',.2f'),format(balance,',.2f')]],
            totals=[('Amount Due (R)',format(balance,',.2f'))])
        documents.extend([dict(slug=slug,label=label,issuer=issuer,document=quote),
            dict(slug=slug,label=label,issuer=issuer,document=statement)])
    return documents


def samples_page():
    slug = request.args.get('trade','mechanic')
    kind = request.args.get('kind','quote')
    documents = sample_documents()
    chosen = next((index for index,item in enumerate(documents) if item['slug']==slug and item['document']['kind']==kind),None)
    if chosen is None:
        abort(404)
    return render_template('program_mechanic/samples.html', samples=SAMPLES,
        selected_trade=slug, selected_kind=kind, page=chosen+1)
