from wsgi import app
from app.extensions import db
from app.models.billing import BilMuniAccount, BilMeter, RefMuniOwner
import json
from datetime import date

def build_wizard_data_from_db(property_id):
    wizardData = {
        'accounts': [],
        'bulkWater': [],
        'bulkElec': [],
        'subWater': [],
        'subElec': [],
        'exceptions': [],
        'mapping': [],
        'rates': [],
        'arrangements': [],
        'owners': [],
        'propertyMap': {}
    }
    
    accounts = BilMuniAccount.query.filter_by(property_id=property_id).order_by(BilMuniAccount.id).all()
    if not accounts:
        return None
        
    acc_map = {}
    bulk_acc_num = None
    
    for i, acc in enumerate(accounts):
        acc_id_str = f"acc_{i}"
        acc_map[acc.account_number] = acc_id_str
        if acc.is_bulk_account:
            bulk_acc_num = acc.account_number
            
        wizardData['accounts'].append({
            'id': acc_id_str,
            'number': acc.account_number,
            'owner': '', # Handled in owners section
            'isBulk': acc.is_bulk_account
        })
        
        # Rates
        if acc.rates_date:
            wizardData['rates'].append({
                'account_id': acc_id_str,
                'amount': acc.rates_amount or 0.0,
                'date': acc.rates_date.strftime("%Y-%m-%d") if acc.rates_date else '',
                'charge_to': acc.rates_charge_to or 'owner',
                'reference': acc.rates_reference or '',
                'erf_details': acc.rates_erf_details or '',
                'property_category': acc.rates_property_category or '',
                'market_value': acc.rates_market_value or 0.0,
                'rateable_value': acc.rates_rateable_value or 0.0,
                'general_randage': acc.rates_general_randage or 0.0,
                'sra_randage': acc.rates_sra_randage or 0.0,
                'deferred': acc.rates_deferred or 0.0,
                'sra_monthly': acc.rates_sra_monthly or 0.0,
                'general_monthly': acc.rates_general_monthly or 0.0
            })
            
        # Arrangements
        if acc.arrangement_date:
            wizardData['arrangements'].append({
                'account_id': acc_id_str,
                'charge_to': acc.arrangement_charge_to or 'owner',
                'contract_number': acc.ca_contract_number or '',
                'agreement_amount': acc.ca_agreement_amount or 0.0,
                'installments_raised': acc.ca_installments_raised or 0.0,
                'installment_amount': acc.ca_installment_amount or 0.0,
                'amount_owing': acc.ca_amount_owing or 0.0,
                'remaining_periods': acc.ca_remaining_periods or 0,
                'date': acc.arrangement_date.strftime("%Y-%m-%d") if acc.arrangement_date else ''
            })
            
        # Owners
        if acc.owner_id:
            owner = RefMuniOwner.query.get(acc.owner_id)
            if owner:
                wizardData['owners'].append({
                    'account_id': acc_id_str,
                    'name': owner.name or '',
                    'email': acc.muni_email or '',
                    'address': acc.owner_address or ''
                })

    acc_nums = [a.account_number for a in accounts if a.account_number]
    meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(acc_nums)).all()
    
    bw_count = 0
    be_count = 0
    sw_count = 0
    se_count = 0
    
    for m in meters:
        u = (m.utility_type or '').lower()
        is_bulk = (m.municipal_bill_number == bulk_acc_num)
        
        if 'water' in u:
            if is_bulk:
                m_id = f"bulk-water_{bw_count}"
                wizardData['bulkWater'].append({'id': m_id, 'number': m.meter_number})
                bw_count += 1
            else:
                m_id = f"sub-water_{sw_count}"
                wizardData['subWater'].append({'id': m_id, 'number': m.meter_number})
                sw_count += 1
        else:
            if is_bulk:
                m_id = f"bulk-elec_{be_count}"
                wizardData['bulkElec'].append({'id': m_id, 'number': m.meter_number})
                be_count += 1
            else:
                m_id = f"sub-elec_{se_count}"
                wizardData['subElec'].append({'id': m_id, 'number': m.meter_number})
                se_count += 1
                
        # Mapping
        if not is_bulk and m.municipal_bill_number in acc_map:
            wizardData['mapping'].append({
                'sub_meter_id': m_id,
                'account_id': acc_map[m.municipal_bill_number]
            })

    wizardData['propertyMap'] = {
        'accounts': len(accounts),
        'water': bw_count + sw_count,
        'elec': be_count + se_count,
        'bulkWater': bw_count > 0,
        'bulkElec': be_count > 0,
        'owners': len(set([o['name'] for o in wizardData['owners'] if o['name']])) or 1,
        'addresses': len(set([o['address'] for o in wizardData['owners'] if o['address']])) or 1
    }
    
    return wizardData

with app.app_context():
    data = build_wizard_data_from_db(22)
    print(json.dumps(data, indent=2))
