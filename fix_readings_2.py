with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

import re

old_meters = """        for m_data in data.get('meters', []):
            m_num = m_data.get('number', '').strip()
            acc_num = m_data.get('account', '').strip()
            u_type = 'Water' if 'water' in m_data.get('type', '').lower() else 'Electrical'
            
            if m_num:
                meter = BilMeter(
                    meter_number=m_num,
                    utility_type=u_type,
                    municipal_bill_number=acc_num
                )
                db.session.add(meter)"""

new_meters = """        for m_data in data.get('meters', []):
            m_num = m_data.get('number', '').strip()
            acc_num = m_data.get('account', '').strip()
            u_type = 'Water' if 'water' in m_data.get('type', '').lower() else 'Electrical'
            
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

text = text.replace(old_meters, new_meters)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated initialReadings logic in save_global_architecture!")
