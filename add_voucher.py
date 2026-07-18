from app.extensions import db
with open('app/models/culturalfire.py', 'r', encoding='utf-8') as f2:
    text = f2.read()
if 'CfiVoucher' not in text:
    text += """
class CfiVoucher(db.Model):
    __tablename__ = 'cfi_voucher'

    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(50), unique=True, nullable=False)
    tokens = db.Column(db.Integer, default=200)
    is_used = db.Column(db.Boolean, default=False)
    used_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    used_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())
"""
    with open('app/models/culturalfire.py', 'w', encoding='utf-8') as f2:
        f2.write(text)
    print('Added CfiVoucher model')
else:
    print('Already exists')
