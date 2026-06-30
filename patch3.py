with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

import re
# We want to replace the end of parse_bill_onboarding_api:
#         data = json.loads(text_response.strip())
#         
#         return jsonify(data)

pattern = re.compile(r'data = json\.loads\(text_response\.strip\(\)\)\s*return jsonify\(data\)')

injection = '''data = json.loads(text_response.strip())
        
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

new_content, count = pattern.subn(injection, content, count=1)
if count > 0:
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Successfully injected BilExtractionLog!")
else:
    print("Failed to find pattern.")
