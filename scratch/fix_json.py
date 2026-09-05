import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace(
    '    from app.utils.mailer import send_email',
    '    from app.utils.mailer import send_email\n    import json'
)

text = text.replace(
    '    from app.models.auth import User',
    '    from app.models.auth import User\n    import json'
)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
