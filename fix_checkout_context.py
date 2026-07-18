import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement = '''
    session["metro_billing_meters"] = meter_count
    session["metro_billing_amount_cents"] = cost_cents
    
    main_prop = props[0] if props else None
    is_trial = main_prop.on_trial if main_prop else False
    
    return render_template("program_billing/checkout_summary.html", 
                           month=month, 
                           meter_count=meter_count, 
                           cost_cents=cost_cents, 
                           settings=settings,
                           main_prop=main_prop,
                           is_trial=is_trial)
'''

# Find the end of billing_checkout
start_idx = text.find('session["metro_billing_meters"] = meter_count')
if start_idx != -1:
    end_idx = text.find('return render_template("program_billing/checkout_summary.html"', start_idx)
    end_idx = text.find(')', end_idx) + 1
    text = text[:start_idx] + replacement.strip() + text[end_idx:]

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Fixed billing_checkout context variables')
