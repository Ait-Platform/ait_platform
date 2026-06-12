import sys
from app import create_app, db

app = create_app()

sql_tenant = """
CREATE TABLE IF NOT EXISTS bil_metsoa_tenant_month (
    id SERIAL PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    month VARCHAR(7) NOT NULL,
    ws_total FLOAT DEFAULT 0,
    sd_total FLOAT DEFAULT 0,
    water_total FLOAT DEFAULT 0,
    updated_at TIMESTAMP,
    CONSTRAINT uq_metsoa_tenant_month UNIQUE (tenant_id, month)
);
"""

sql_meter = """
CREATE TABLE IF NOT EXISTS bil_metsoa_meter_month (
    id SERIAL PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    meter_id INTEGER NOT NULL,
    month VARCHAR(7) NOT NULL,
    utility_type VARCHAR(50),
    prev_date VARCHAR(50),
    prev_read FLOAT,
    curr_date VARCHAR(50),
    curr_read FLOAT,
    days INTEGER,
    consumption FLOAT,
    elec_rate FLOAT,
    elec_due FLOAT,
    ws_total FLOAT DEFAULT 0,
    sd_total FLOAT DEFAULT 0,
    water_cost FLOAT DEFAULT 0,
    total_due FLOAT DEFAULT 0,
    updated_at TIMESTAMP,
    CONSTRAINT uq_metsoa_meter_month UNIQUE (tenant_id, meter_id, month)
);
"""

with app.app_context():
    try:
        db.session.execute(db.text(sql_tenant))
        db.session.execute(db.text(sql_meter))
        db.session.commit()
        print("Created METSOA tables successfully!")
    except Exception as e:
        db.session.rollback()
        print("Error creating tables:", e)
        sys.exit(1)
