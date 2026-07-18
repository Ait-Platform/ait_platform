import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement_old = '''    main_prop = props[0] if props else None
    is_trial = main_prop.on_trial if main_prop else False'''

replacement_new = '''    main_prop = props[0] if props else None
    from datetime import datetime
    is_trial = main_prop.trial_ends_at and main_prop.trial_ends_at > datetime.utcnow() if main_prop else False'''

text = text.replace(replacement_old, replacement_new)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Fixed is_trial logic')
