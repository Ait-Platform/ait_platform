from app import create_app
from app.extensions import db
from app.models.debtors import BusinessBankAccount
import sys

app = create_app()
with app.app_context():
    banks = BusinessBankAccount.query.all()
    print(f"Total Bank Accounts: {len(banks)}")
    for bank in banks:
        print(f"Bank ID: {bank.id}, User ID: {bank.user_id}, Name: {bank.bank_name}, Default: {bank.is_default}")
