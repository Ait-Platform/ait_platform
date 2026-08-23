import re

with open('app/__init__.py', 'r', encoding='utf-8') as f:
    content = f.read()

inject = '''        try:
            db.session.execute(text("ALTER TABLE soa_profile ADD COLUMN letterhead_url VARCHAR(255);"))
            db.session.commit()
        except Exception:
            db.session.rollback()

        try:
            db.session.execute(text("ALTER TABLE soa_profile ADD COLUMN use_custom_letterhead BOOLEAN DEFAULT FALSE;"))
            db.session.commit()
        except Exception:
            db.session.rollback()

'''

content = content.replace('        try:\n            db.session.execute(text("ALTER TABLE sender_profile ADD COLUMN letterhead_url VARCHAR(255);"))', inject + '        try:\n            db.session.execute(text("ALTER TABLE sender_profile ADD COLUMN letterhead_url VARCHAR(255);"))')

with open('app/__init__.py', 'w', encoding='utf-8') as f:
    f.write(content)
