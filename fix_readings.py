import re
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement = '''
    prev_reading = data.get("prev_reading")
    prev_date_str = data.get("prev_date")
    
    if prev_reading is not None and prev_reading != "":
        prev_reading = float(prev_reading)
        if prev_date_str:
            prev_date = datetime.strptime(prev_date_str, "%Y-%m-%d").date()
        else:
            from dateutil.relativedelta import relativedelta
            prev_date = new_date - relativedelta(days=30)
    else:
        # Query the database for the most recent reading before new_date
        from app.models.billing import BilMeterReading
        last_r = BilMeterReading.query.filter(
            BilMeterReading.meter_id == m.id,
            BilMeterReading.reading_date < new_date
        ).order_by(BilMeterReading.reading_date.desc()).first()
        
        if last_r:
            prev_reading = last_r.reading_value
            prev_date = last_r.reading_date
        else:
            prev_reading = 0
            from dateutil.relativedelta import relativedelta
            prev_date = new_date - relativedelta(days=30)
'''

text = re.sub(
    r'prev_reading = float\(data\.get\("prev_reading", 0\)\) if data\.get\("prev_reading"\) else 0\s+prev_date_str = data\.get\("prev_date"\)\s+if prev_date_str:\s+prev_date = datetime\.strptime\(prev_date_str, "%Y-%m-%d"\)\.date\(\)\s+else:\s+from dateutil\.relativedelta import relativedelta\s+prev_date = new_date - relativedelta\(days=30\)',
    replacement.strip(),
    text,
    flags=re.DOTALL
)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print('Done updating save_reading in routes.py')
