from sqlalchemy import text
from app.extensions import db

def get_active_countries():
    """
    Returns a list of dictionaries with 'code' and 'name' for all active countries,
    ordered alphabetically by name.
    """
    return db.session.execute(
        text("""
            SELECT r.alpha2 AS code, r.name
              FROM ref_country_currency r
             WHERE (r.is_active IS NULL OR r.is_active::text IN ('1','t','true','TRUE'))
             ORDER BY r.name
        """)
    ).mappings().all()
