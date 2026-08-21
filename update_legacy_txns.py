from app import create_app
from app.extensions import db
from app.models.auth import AitTokenTransaction

app = create_app()
with app.app_context():
    # Find transactions matching 'Generated quote for shop %'
    txns = AitTokenTransaction.query.filter(AitTokenTransaction.description.like('Generated quote for shop %')).all()
    count = 0
    for txn in txns:
        txn.description = "Generated quote (Legacy)"
        count += 1
    
    db.session.commit()
    print(f"Updated {count} legacy transactions.")
