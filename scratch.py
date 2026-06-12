from sqlalchemy import create_engine, text
db_url = 'postgresql+psycopg2://ait_local:temp1234@localhost:5432/ait_local_db'
engine = create_engine(db_url)
with engine.connect() as conn:
    conn.execute(text("UPDATE cfi_pageant_segments SET name = 'Traditional Wear' WHERE name ILIKE '%eastern%'"))
    conn.execute(text("UPDATE cfi_pageant_segments SET name = 'Formal Wear' WHERE name ILIKE '%western%'"))
    conn.execute(text("UPDATE cfi_segment_items SET segment_type = 'Traditional Wear', title = 'Traditional Wear' WHERE segment_type ILIKE '%eastern%'"))
    conn.execute(text("UPDATE cfi_segment_items SET segment_type = 'Formal Wear', title = 'Formal Wear' WHERE segment_type ILIKE '%western%'"))
    conn.commit()
    print("Database updated successfully.")
