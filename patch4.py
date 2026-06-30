import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# First, remove the mistaken injection from parse_readings_api
wrong_target = '''        try:
            from app.models.billing import BilExtractionLog
            from flask_login import current_user
            
            if current_user.is_authenticated:
                def _safe_float(val):
                    try:
                        return float(val) if val is not None else 0.0
                    except (ValueError, TypeError):
                        return 0.0
                        
                log_entry = BilExtractionLog(
                    manager_id=current_user.id,
                    property_name=data.get("property_name"),
                    address=data.get("address"),
                    metro_account_no=data.get("metro_account_no"),
                    muni_email=data.get("muni_email"),
                    has_rates=bool(data.get("has_rates")),
                    rates_amount=_safe_float(data.get("rates_amount")),
                    amount_due=_safe_float(data.get("amount_due")),
                    raw_json=data
                )
                from app.extensions import db
                db.session.add(log_entry)
                db.session.commit()
        except Exception as inner_e:
            import logging
            logging.error(f"Failed to save BilExtractionLog: {inner_e}")'''

if wrong_target in content:
    content = content.replace(wrong_target, "")
    print("Removed wrong injection!")

# Now, inject it correctly into parse_bill_onboarding_api
# It is located around line 2413:
#         data = json.loads(text_response.strip())
#         
#         
#         return jsonify(data)

target = '''        data = json.loads(text_response.strip())
        
        
        return jsonify(data)'''

injection = '''        data = json.loads(text_response.strip())
        
        try:
            from app.models.billing import BilExtractionLog
            from flask_login import current_user
            
            if current_user.is_authenticated:
                def _safe_float(val):
                    try:
                        return float(val) if val is not None else 0.0
                    except (ValueError, TypeError):
                        return 0.0
                        
                log_entry = BilExtractionLog(
                    manager_id=current_user.id,
                    property_name=data.get("property_name"),
                    address=data.get("address"),
                    metro_account_no=data.get("metro_account_no"),
                    muni_email=data.get("muni_email"),
                    has_rates=bool(data.get("has_rates")),
                    rates_amount=_safe_float(data.get("rates_amount")),
                    amount_due=_safe_float(data.get("amount_due")),
                    raw_json=data
                )
                from app.extensions import db
                db.session.add(log_entry)
                db.session.commit()
        except Exception as inner_e:
            import logging
            logging.error(f"Failed to save BilExtractionLog: {inner_e}")
            
        return jsonify(data)'''

if target in content:
    content = content.replace(target, injection, 1)
    print("Successfully injected into parse_bill_onboarding_api!")
else:
    print("FAILED to find target in parse_bill_onboarding_api!")

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
