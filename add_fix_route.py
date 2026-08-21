import re

with open('app/public/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

route = '''
@public_bp.route("/run_db_fix_sender")
def run_db_fix_sender():
    try:
        db.session.execute(text('ALTER TABLE sender_profile ADD COLUMN letterhead_url VARCHAR(255)'))
    except Exception as e:
        db.session.rollback()
    try:
        db.session.execute(text('ALTER TABLE sender_profile ADD COLUMN use_custom_letterhead BOOLEAN DEFAULT FALSE'))
    except Exception as e:
        db.session.rollback()
    db.session.commit()
    return "DB Fix executed successfully. Please go back to the app."
'''

content = content + '\n' + route

with open('app/public/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
