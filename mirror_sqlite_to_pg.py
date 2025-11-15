import os
from sqlalchemy import create_engine, MetaData, Table, text

# --- 1) CONFIG -------------------------------------------------------

SQLITE_PATH = r"D:\Users\yeshk\Documents\ait_platform\instance\data.db"
SQLITE_URL = f"sqlite:///{SQLITE_PATH}"

PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)

print(f"Using SQLite: {SQLITE_PATH}")

sqlite_engine = create_engine(SQLITE_URL)
pg_engine = create_engine(PG_URL)

# We only mirror these three tables
GOOD_TABLES = ["user", "auth_pricing", "user_enrollment"]

# Delete children → parents (FK-safe)
DELETE_ORDER = ["user_enrollment", "auth_pricing", "user"]

# Insert parents → children
INSERT_ORDER = ["user", "auth_pricing", "user_enrollment"]

metadata = MetaData()

# Reflect ONLY these tables from SQLite
for name in GOOD_TABLES:
    Table(name, metadata, autoload_with=sqlite_engine)

# Make sure PG has the same tables (already created by app/migrations)
metadata.create_all(pg_engine)

# --- 2) Build subject ID mapping (SQLite → PG via slug) ---------------

with sqlite_engine.connect() as s_conn:
    sqlite_subjects = s_conn.execute(
        text('SELECT id, slug FROM auth_subject')
    ).mappings().all()

sqlite_id_to_slug = {row["id"]: row["slug"] for row in sqlite_subjects}

with pg_engine.connect() as p_conn:
    pg_subjects = p_conn.execute(
        text('SELECT id, slug FROM auth_subject')
    ).mappings().all()

slug_to_pg_id = {row["slug"]: row["id"] for row in pg_subjects}

print("SQLite subject id→slug:", sqlite_id_to_slug)
print("PG slug→id:", slug_to_pg_id)

# --- 3) COPY ROWS -----------------------------------------------------

with sqlite_engine.connect() as s_conn, pg_engine.begin() as p_conn:
    # 3a) Clear PG tables in FK-safe order
    for name in DELETE_ORDER:
        table = metadata.tables[name]
        print(f"\nClearing {name} in PostgreSQL…")
        p_conn.execute(table.delete())

    # 3b) Insert data with subject_id remapped where needed
    for name in INSERT_ORDER:
        table = metadata.tables[name]
        print(f"\n--- {name} ---")

        count = s_conn.execute(
            text(f'SELECT COUNT(*) FROM "{name}"')
        ).scalar()
        print(f"SQLite rows: {count}")

        rows = s_conn.execute(table.select()).mappings().all()

        fixed_rows = []
        for row in rows:
            row = dict(row)  # make it mutable

            if name in ("auth_pricing", "user_enrollment"):
                sid_sqlite = row.get("subject_id")
                slug = sqlite_id_to_slug.get(sid_sqlite)
                pg_sid = slug_to_pg_id.get(slug) if slug else None

                if not slug or not pg_sid:
                    print(
                        f"  - SKIP row with subject_id={sid_sqlite} "
                        f"(slug={slug!r} not found in PG)"
                    )
                    continue

                row["subject_id"] = pg_sid

            fixed_rows.append(row)

        if fixed_rows:
            p_conn.execute(table.insert(), fixed_rows)
            print(f"Inserted into PG: {len(fixed_rows)}")
        else:
            print("Inserted into PG: 0")

print("\n✅ Done — user, auth_pricing and user_enrollment mirrored to PostgreSQL (with subject_ids remapped by slug).")
