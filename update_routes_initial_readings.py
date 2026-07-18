import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fetch initialReadings
old_fetch = """        exceptions = data.get('exceptions', [])
        mapping = data.get('mapping', [])
        arrears = data.get('arrears', [])
        arrangements = data.get('arrangements', [])
        owners = data.get('owners', [])"""

new_fetch = """        exceptions = data.get('exceptions', [])
        mapping = data.get('mapping', [])
        arrears = data.get('arrears', [])
        arrangements = data.get('arrangements', [])
        owners = data.get('owners', [])
        initial_readings = data.get('initialReadings', [])"""

content = content.replace(old_fetch, new_fetch)

# 2. Add baseline readings for ALL meters
old_exceptions = """        # 4. Handle Exceptions (Stolen Meters)"""

new_exceptions = """        # 3.5 Handle Initial Readings
        from datetime import datetime
        for read_data in initial_readings:
            m_num = read_data.get('meter_number')
            m_val = read_data.get('value')
            m_date_str = read_data.get('date')
            
            if m_num and m_val is not None:
                try:
                    m_val = float(m_val)
                except ValueError:
                    m_val = 0.0
                
                m_date = datetime.utcnow().date()
                if m_date_str:
                    try: m_date = datetime.strptime(m_date_str, '%Y-%m-%d').date()
                    except ValueError: pass
                    
                # Find the meter we just inserted
                target_meter = next((m for m in db_meters.values() if m.meter_number == m_num), None)
                if target_meter:
                    # Create a baseline BilMeterReading
                    base_reading = BilMeterReading(
                        meter_id=target_meter.id,
                        reading_date=m_date,
                        reading_value=m_val
                    )
                    db.session.add(base_reading)
                    
                    # Also create a baseline Consumption record (days=0, consumption=0) so MetSOA has a starting point
                    # We use BilConsumption directly so it shows up in history
                    from app.models.billing import BilConsumption
                    base_cons = BilConsumption(
                        meter_id=target_meter.id,
                        meter_number=target_meter.meter_number,
                        month=m_date.strftime('%Y-%m'),
                        last_date=m_date,
                        new_date=m_date,
                        last_read=m_val,
                        new_read=m_val,
                        days=0,
                        consumption=0.0
                    )
                    db.session.add(base_cons)
        
        # 4. Handle Exceptions (Stolen Meters)"""

content = content.replace(old_exceptions, new_exceptions)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("routes.py updated with initial readings.")
