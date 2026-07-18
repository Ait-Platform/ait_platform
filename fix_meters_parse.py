with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'for m_data in data\.get\(\'meters\', \[\]\):.*?pass', text, re.DOTALL)
if m:
    old_code = m.group(0)
    new_code = """# Extract meters and map them properly from frontend payload
        raw_meters = []
        for u_type, key in [('Water', 'bulkWater'), ('Electrical', 'bulkElec'), ('Water', 'subWater'), ('Electrical', 'subElec')]:
            for m_item in data.get(key, []):
                m_item['u_type'] = u_type
                raw_meters.append(m_item)
                
        # Build account map { account_id: account_number }
        acc_map = { a.get('id'): a.get('number', '').strip() for a in data.get('accounts', []) }
        
        # Build meter-to-account map { meter_id: account_number }
        meter_acc = {}
        for mp in data.get('mapping', []):
            acc_id = mp.get('account_id')
            meter_id = mp.get('meter_id')
            if acc_id in acc_map:
                meter_acc[meter_id] = acc_map[acc_id]
                
        for m_data in raw_meters:
            m_num = m_data.get('number', '').strip()
            m_id = m_data.get('id')
            acc_num = meter_acc.get(m_id, '')
            u_type = m_data.get('u_type')
            
            if m_num:
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
                                parsed_date = datetime.strptime(rd_date, '%Y-%m-%d').date()
                                reading = BilMeterReading(
                                    meter_id=meter.id,
                                    reading_date=parsed_date,
                                    reading_value=float(rd_val)
                                )
                                db.session.add(reading)
                            except:
                                pass"""
    
    text = text.replace(old_code, new_code)
    
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed meters gathering!")
else:
    print("Regex failed to match!")
