"""Additive ProTrade price-list schema; no legacy rows are rewritten."""
from sqlalchemy import text


def ensure_price_list_schema(engine):
    with engine.begin() as conn:
        # Verify the connection independently before executing DDL.
        identity = conn.execute(text('SELECT current_database(), current_user')).one()
        if not all(identity):
            raise RuntimeError('Cannot verify price-list database connection')
        conn.execute(text("ALTER TABLE mech_catalog_parts ADD COLUMN IF NOT EXISTS shop_id INTEGER REFERENCES mech_shops(id)"))
        conn.execute(text("ALTER TABLE mech_catalog_parts ADD COLUMN IF NOT EXISTS trade VARCHAR(50)"))
