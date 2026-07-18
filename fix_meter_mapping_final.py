with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

import re

old_block = re.search(r'# Extract meters and map them properly from frontend payload.*?try:\s*parsed_date = datetime\.strptime', text, re.DOTALL)

if old_block:
    new_code = """# Extract meters and map them properly from frontend payload
        raw_meters = []
        for u_type, key in [('Water', 'bulkWater'), ('Electrical', 'bulkElec'), ('Water', 'subWater'), ('Electrical', 'subElec')]:
            for m_item in data.get(key, []):
                m_item['u_type'] = u_type
                m_item['is_bulk'] = key.startswith('bulk')
                raw_meters.append(m_item)
                
        # Build account map
        acc_map = { a.get('id'): a.get('number', '').strip() for a in data.get('accounts', []) }
        
        # Find the bulk account
        bulk_acc_num = ''
        for a in data.get('accounts', []):
            if a.get('isBulk'):
                bulk_acc_num = a.get('number', '').strip()
                break
        if not bulk_acc_num and data.get('accounts'):
            bulk_acc_num = data.get('accounts')[0].get('number', '').strip()
        
        # Build meter-to-account map { meter_id: account_number }
        meter_acc = {}
        for mp in data.get('mapping', []):
            acc_id = mp.get('account_id')
            w_id = mp.get('water')
            e_id = mp.get('elec')
            if acc_id in acc_map:
                if w_id:
                    meter_acc[w_id] = acc_map[acc_id]
                if e_id:
                    meter_acc[e_id] = acc_map[acc_id]
                
        for m_data in raw_meters:
            m_num = m_data.get('number', '').strip()
            m_id = m_data.get('id')
            u_type = m_data.get('u_type')
            
            # Determine account number
            acc_num = bulk_acc_num if m_data.get('is_bulk') else meter_acc.get(m_id, '')
            
            if m_num and acc_num:
                meter = BilMeter(
                    meter_number=m_num,
                    utility_type=u_type,
                    municipal_bill_number=acc_num
                )
                db.session.add(meter)
                db.session.flush() # get meter.id
                
                initial_readings = data.get('initialReadings', [])
                for r_data in initial_readings:
                    if str(r_data.get('meter_number', '')) == str(m_num):
                        from datetime import datetime
                        from app.models.billing import BilMeterReading
                        rd_date = r_data.get('date')
                        rd_val = r_data.get('value')
                        if rd_date and rd_val is not None:
                            try:
                                parsed_date = datetime.strptime"""
    
    text = text.replace(old_block.group(0), new_code)
    
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed meters mapping!")
else:
    print("Regex failed to match old block.")
