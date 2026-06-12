import sqlite3
import os

db_path = r'D:\Users\yeshk\Documents\ait_platform\instance\data.db'
if not os.path.exists(db_path):
    print("Database not found at", db_path)
else:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    try:
        c.execute('''
        CREATE TABLE IF NOT EXISTS bil_bank_detail (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_name VARCHAR(100) NOT NULL,
            branch_name VARCHAR(100),
            branch_code VARCHAR(50),
            account_number VARCHAR(100) NOT NULL,
            account_holder VARCHAR(150),
            account_type VARCHAR(50)
        )
        ''')
        c.execute('''
        ALTER TABLE bil_tenant ADD COLUMN bank_detail_id INTEGER REFERENCES bil_bank_detail(id)
        ''')
        conn.commit()
        print("Successfully updated database schema.")
    except Exception as e:
        print("Schema update error or already applied:", e)
    finally:
        conn.close()

# Update the models file
import re
models_path = r'D:\Users\yeshk\Documents\ait_platform\app\models\billing.py'
with open(models_path, 'r', encoding='utf-8') as f:
    content = f.read()

if 'class BilBankDetail' not in content:
    bank_detail_code = '''
class BilBankDetail(db.Model):
    __tablename__ = "bil_bank_detail"

    id = db.Column(db.Integer, primary_key=True)
    bank_name = db.Column(db.String(100), nullable=False)
    branch_name = db.Column(db.String(100))
    branch_code = db.Column(db.String(50))
    account_number = db.Column(db.String(100), nullable=False)
    account_holder = db.Column(db.String(150))
    account_type = db.Column(db.String(50))

    def __repr__(self):
        return f"<BilBankDetail {self.bank_name} - {self.account_number}>"
'''
    # Insert before BilTenant
    content = content.replace('class BilTenant(db.Model):', bank_detail_code + '\nclass BilTenant(db.Model):')

if 'bank_detail_id' not in content:
    tenant_replacement = '''    bank_detail_id = db.Column(db.Integer, db.ForeignKey('bil_bank_detail.id'), nullable=True)
    bank_detail = db.relationship("BilBankDetail")

    notes                = db.Column(db.Text)'''
    content = content.replace('    notes                = db.Column(db.Text)', tenant_replacement)

with open(models_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated billing.py")
