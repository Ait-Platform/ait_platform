from app import create_app
from app.extensions import db
from app.models.mechanic import MechShop, MechClient
from app.models.debtors import BusinessBankAccount
import sys

app = create_app()
with app.app_context():
    shop = MechShop.query.first()
    if not shop:
        print("No shop found")
        sys.exit(0)
    print(f"Shop user_id: {shop.user_id}")
    print(f"Shop bank_details: {shop.bank_details}")
    
    bank = BusinessBankAccount.query.filter_by(user_id=shop.user_id).first()
    if bank:
        print(f"Bank Account Found! Bank Name: {bank.bank_name}, Account Name: {bank.account_name}, raw_details: {bank.raw_details}")
    else:
        print("No Bank Account Found in BusinessBankAccount!")
