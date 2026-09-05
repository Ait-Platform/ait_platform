from sqlalchemy import text

@uip_bp.route("/_db_fix")
def fix_db():
    from app.extensions import db
    
    # 1. Ensure all new tables are created (db.create_all ignores existing tables)
    db.create_all()
    
    # 2. Add missing columns to core_organization if they don't exist
    columns_to_add = [
        "area VARCHAR(255)",
        "municipality_ref VARCHAR(255)",
        "contact_email VARCHAR(255)",
        "contact_phone VARCHAR(50)",
        "status VARCHAR(50) DEFAULT 'active'",
        "config_json TEXT"
    ]
    
    results = []
    for col in columns_to_add:
        col_name = col.split()[0]
        try:
            # Check if column exists
            db.session.execute(text(f"SELECT {col_name} FROM core_organization LIMIT 1"))
            results.append(f"Column {col_name} already exists.")
        except Exception as e:
            db.session.rollback()
            try:
                # Add column
                db.session.execute(text(f"ALTER TABLE core_organization ADD COLUMN {col}"))
                db.session.commit()
                results.append(f"Added column {col_name} successfully.")
            except Exception as inner_e:
                db.session.rollback()
                results.append(f"Failed to add column {col_name}: {str(inner_e)}")
                
    return "<br>".join(results) + "<br><br><a href='/uip/manor-gardens/dashboard'>Go to Dashboard</a>"
