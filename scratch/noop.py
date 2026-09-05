import sys

# Replace the previous fix script with a comprehensive one that also seeds Manor Gardens
with open('app/uip/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace(\"""@uip_bp.route("/_db_fix")
def fix_db():\""", \"""@uip_bp.route("/_db_fix")
def fix_db():
    from app.extensions import db
    from app.models.core import CoreOrganization, CoreOrganizationWallet, CoreOrganizationLedger
    from sqlalchemy import text\""")

# Wait, instead of complex string replacement, I can just append another route
