from wsgi import app
from app.extensions import db
from app.models.billing import BilMeter, BilMuniAccount
from flask import render_template
with app.app_context():
    with app.test_request_context('/'):
        prop_id = 32
        accounts = BilMuniAccount.query.filter_by(property_id=prop_id).all()
        acc_nums = [a.account_number for a in accounts if a.account_number]
        meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(acc_nums)).all()
        
        html = render_template('program_billing/architecture_summary.html',
                               property=type('Obj', (object,), {'id': prop_id, 'name': 'Dale', 'address': '123', 'erf_number': '456', 'manager_id': 1})(),
                               accounts=accounts,
                               meters=meters,
                               bulk_water=[], bulk_elec=[], sub_water=[], sub_elec=[],
                               exceptions=[], owners=[])
        
        import re
        m = re.search(r'<tbody[^>]*>(.*?)</tbody>', html, re.DOTALL)
        if m:
            body = m.group(1)
            body = re.sub(r'<[^>]+>', ' ', body)
            body = re.sub(r'\s+', ' ', body)
            print('RENDERED OUTPUT:', body.strip())
