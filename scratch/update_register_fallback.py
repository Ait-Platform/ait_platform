import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_fallback = """        try:
            from app.payments.pricing import currency_for_country_code
            currency = currency_for_country_code(country) or "ZAR"
            amount_cents = price_cents_for(subject, currency)
        except Exception:
            current_app.logger.exception(
                "pricing failed for subject=%s country=%s currency=%s", subject, country, currency
            )
            amount_cents = None"""

new_fallback = """        try:
            from app.models.auth import AuthSubject
            subj_obj_for_price = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == subject.lower()).first()
            
            amount_cents = None
            if subj_obj_for_price:
                from app.payments.pricing import price_for_country
                local_cents, zar_cents, currency = price_for_country(subj_obj_for_price.id, country)
                if local_cents and local_cents > 0:
                    amount_cents = local_cents
            
            if not amount_cents:
                from app.payments.pricing import currency_for_country_code
                currency = currency_for_country_code(country) or "ZAR"
                amount_cents = price_cents_for(subject, currency)
        except Exception:
            current_app.logger.exception(
                "pricing failed for subject=%s country=%s", subject, country
            )
            amount_cents = None"""

content = content.replace(old_fallback, new_fallback)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated register_decision fallback to check subject_country_price")
