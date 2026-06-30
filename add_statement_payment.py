with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_model = '''
class BilStatementPayment(db.Model):
    __tablename__ = 'bil_statement_payment'
    id = db.Column(db.Integer, primary_key=True)
    manager_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    month = db.Column(db.String(7), nullable=False)  # 'YYYY-MM'
    meters_billed = db.Column(db.Integer, nullable=False, default=0)
    amount_paid_cents = db.Column(db.Integer, nullable=False, default=0)
    paid_at = db.Column(db.DateTime, default=func.now())
    
    __table_args__ = (db.UniqueConstraint('manager_id', 'month', name='uq_manager_month_payment'),)
'''

if 'BilStatementPayment' not in text:
    text += new_model
    with open('app/models/billing.py', 'w', encoding='utf-8') as f:
        f.write(text)

    from app.extensions import db
    from app import create_app
    app = create_app()
    with app.app_context():
        db.create_all()

    print('Updated billing models and created BilStatementPayment table.')
else:
    print('BilStatementPayment already exists.')
