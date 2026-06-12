import sys
import os

# Add the project root to the python path so we can import 'app'
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from app import create_app, db
from app.models.billing import RefMuniOwner
from sqlalchemy import text

app = create_app()

def run_migration():
    with app.app_context():
        print("Creating all missing models in Postgres...")
        db.create_all()

        print("Seeding initial owners...")
        for owner_name in ['S. Nanhoo', '<Other Owner>']:
            if not RefMuniOwner.query.filter_by(name=owner_name).first():
                owner = RefMuniOwner(name=owner_name)
                db.session.add(owner)
        
        db.session.commit()

        print("Creating views...")
        
        # Drop views if they exist
        db.session.execute(text("DROP VIEW IF EXISTS v_muni_due_vs_metsoa;"))
        db.session.execute(text("DROP VIEW IF EXISTS v_admin_muni_ledger;"))

        v_admin_muni_ledger = """
        CREATE VIEW v_admin_muni_ledger AS
        SELECT
          a.account_number,
          t.period,
          t.balance,
          t.due,
          t.paid,
          t.arrears
        FROM bil_muni_cycle_totals t
        JOIN bil_muni_account a ON a.id = t.account_id;
        """
        db.session.execute(text(v_admin_muni_ledger))

        v_muni_due_vs_metsoa = """
        CREATE VIEW v_muni_due_vs_metsoa AS
        SELECT
          a.account_number,
          t.period,
          t.due        AS system_due,
          m.metsoa_due AS metro_due,
          ROUND(CAST(COALESCE(t.due,0)-COALESCE(m.metsoa_due,0) AS numeric), 2) AS diff
        FROM bil_muni_account a
        LEFT JOIN bil_muni_cycle_totals t
          ON t.account_id = a.id
        LEFT JOIN bil_metsoa_cycle m
          ON m.account_id = a.id AND m.period = t.period;
        """
        db.session.execute(text(v_muni_due_vs_metsoa))

        db.session.commit()
        print("✅ Migration complete.")

if __name__ == "__main__":
    run_migration()
