# migrate_muni_schema.py

import os, sqlite3, datetime  # keep all three

# Project root: ...\ait_platform
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Instance folder at project root (Flask best practice)
INSTANCE_DIR = os.path.join(PROJECT_ROOT, "instance")
os.makedirs(INSTANCE_DIR, exist_ok=True)   # ensure it exists

DB_PATH = os.path.join(INSTANCE_DIR, "data.db")

# helpful debug so you can see exactly what path is used
print(f"[migrate] Using DB_PATH: {DB_PATH}")
print(f"[migrate] Instance dir exists: {os.path.isdir(INSTANCE_DIR)}")




def q(conn, sql, params=()):
    cur = conn.execute(sql, params)
    return cur

def table_exists(conn, name):
    return q(conn, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,)).fetchone() is not None

def view_exists(conn, name):
    return q(conn, "SELECT 1 FROM sqlite_master WHERE type='view' AND name=?;", (name,)).fetchone() is not None

def columns(conn, table):
    rows = q(conn, f"PRAGMA table_info('{table}')").fetchall()
    return {r[1] for r in rows}  # column name is at index 1

def ensure_ref_owner(conn):
    q(conn, """
        CREATE TABLE IF NOT EXISTS ref_muni_owner (
          id    INTEGER PRIMARY KEY AUTOINCREMENT,
          name  TEXT NOT NULL UNIQUE
        );
    """)
    # seed the two known owners
    q(conn, "INSERT OR IGNORE INTO ref_muni_owner(name) VALUES ('S. Nanhoo');")
    q(conn, "INSERT OR IGNORE INTO ref_muni_owner(name) VALUES ('<Other Owner>');")

def backup_table(conn, src):
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    bkp = f"{src}__backup_{ts}"
    if table_exists(conn, src):
        # create a physical backup copy of existing data
        q(conn, f"CREATE TABLE {bkp} AS SELECT * FROM {src};")
        return bkp
    return None

def drop_view_if_exists(conn, name):
    if view_exists(conn, name):
        q(conn, f"DROP VIEW {name};")

def recreate_core_views(conn):
    drop_view_if_exists(conn, "v_admin_muni_ledger")
    drop_view_if_exists(conn, "v_muni_due_vs_metsoa")
    q(conn, """
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
    """)
    q(conn, """
        CREATE VIEW v_muni_due_vs_metsoa AS
        SELECT
          a.account_number,
          t.period,
          t.due        AS system_due,
          m.metsoa_due AS metro_due,
          ROUND(COALESCE(t.due,0)-COALESCE(m.metsoa_due,0), 2) AS diff
        FROM bil_muni_account a
        LEFT JOIN bil_muni_cycle_totals t
          ON t.account_id = a.id
        LEFT JOIN bil_metsoa_cycle m
          ON m.account_id = a.id AND m.period = t.period;
    """)

def create_new_accounts_schema(conn):
    # Always start clean so reruns never fail
    q(conn, "DROP TABLE IF EXISTS bil_muni_account_new;")
    q(conn, """
        CREATE TABLE bil_muni_account_new (
          id                   INTEGER PRIMARY KEY AUTOINCREMENT,
          account_number       TEXT NOT NULL UNIQUE,
          owner_id             INTEGER NOT NULL,
          email_electric       TEXT,
          water_meter_id       INTEGER,
          elec_meter_id        INTEGER,
          muni_water_meter_no  TEXT,
          muni_water_ref       TEXT,
          muni_elec_meter_no   TEXT,
          muni_elec_ref        TEXT,
          FOREIGN KEY(owner_id)        REFERENCES ref_muni_owner(id) ON DELETE RESTRICT,
          FOREIGN KEY(water_meter_id)  REFERENCES bil_meter(id)      ON DELETE SET NULL,
          FOREIGN KEY(elec_meter_id)   REFERENCES bil_meter(id)      ON DELETE SET NULL
        );
    """)


def migrate_old_accounts(conn, backup_name):
    """
    Try to read rows from the most likely old table (either the original or its backup),
    mapping known legacy column names -> new schema.
    """
    source = None
    if table_exists(conn, "bil_muni_account"):
        source = "bil_muni_account"
    elif backup_name and table_exists(conn, backup_name):
        source = backup_name

    if not source:
        return  # nothing to migrate

    src_cols = columns(conn, source)

    # accepted aliases for each target field
    aliases = {
        "account_number": ["account_number", "AccNum", "AccountNo", "Acc_Number"],
        "owner_name":     ["owner_name", "MetOwnerID_FK", "Owner", "OwnerName"],
        "email_electric": ["email_electric", "EmailEle", "Email"],
        "muni_water_meter_no": ["muni_water_meter_no", "WtrMtr", "WaterMeter"],
        "muni_water_ref":      ["muni_water_ref", "WtrMetRef", "WaterRef"],
        "muni_elec_meter_no":  ["muni_elec_meter_no", "EleMtr", "ElecMeter"],
        "muni_elec_ref":       ["muni_elec_ref", "EleMetRef", "ElecRef"],
    }

    # build a SELECT list using the first matching alias that exists in the source table
    select_exprs = {}
    for target, opts in aliases.items():
        pick = next((c for c in opts if c in src_cols), None)
        select_exprs[target] = pick  # can be None

    # fetch all rows as dictionaries
    conn.row_factory = sqlite3.Row
    rows = q(conn, f"SELECT * FROM {source}").fetchall()

    # ensure an owner lookup exists; add any unseen owners automatically
    for r in rows:
        owner_name = None
        c = select_exprs["owner_name"]
        if c: owner_name = r[c]
        if owner_name and owner_name.strip():
            q(conn, "INSERT OR IGNORE INTO ref_muni_owner(name) VALUES (?);", (owner_name.strip(),))

    # insert into new table
    for r in rows:
        def grab(key):
            c = select_exprs[key]
            return (r[c] if c and c in r.keys() else None)

        account_number = grab("account_number")
        if not account_number:
            continue  # skip rows without a key

        owner_name = grab("owner_name") or "S. Nanhoo"
        owner_id = q(conn, "SELECT id FROM ref_muni_owner WHERE name=?;", (owner_name,)).fetchone()
        owner_id = owner_id[0] if owner_id else q(conn, "SELECT id FROM ref_muni_owner WHERE name='S. Nanhoo'").fetchone()[0]

        q(conn, """
            INSERT OR IGNORE INTO bil_muni_account_new
              (account_number, owner_id, email_electric,
               water_meter_id, elec_meter_id,
               muni_water_meter_no, muni_water_ref,
               muni_elec_meter_no,  muni_elec_ref)
            VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?);
        """, (
            account_number,
            owner_id,
            grab("email_electric"),
            grab("muni_water_meter_no"),
            grab("muni_water_ref"),
            grab("muni_elec_meter_no"),
            grab("muni_elec_ref"),
        ))

def swap_in_new_accounts(conn):
    # Drop old table if it exists, then rename _new -> real
    if table_exists(conn, "bil_muni_account"):
        q(conn, "DROP TABLE bil_muni_account;")
    q(conn, "ALTER TABLE bil_muni_account_new RENAME TO bil_muni_account;")
    q(conn, "CREATE INDEX IF NOT EXISTS idx_muni_account_owner ON bil_muni_account(owner_id);")

def ensure_monthly_tables(conn):
    q(conn, """
        CREATE TABLE IF NOT EXISTS bil_muni_cycle_totals (
          id         INTEGER PRIMARY KEY AUTOINCREMENT,
          account_id INTEGER NOT NULL REFERENCES bil_muni_account(id) ON DELETE CASCADE,
          period     TEXT    NOT NULL,   -- 'YYYY-MM'
          balance    REAL    NOT NULL,
          due        REAL    NOT NULL,
          arrears    REAL    NOT NULL,
          paid       REAL    NOT NULL,
          UNIQUE (account_id, period)
        );
    """)
    q(conn, """
        CREATE TABLE IF NOT EXISTS bil_metsoa_cycle (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          account_id  INTEGER NOT NULL REFERENCES bil_muni_account(id) ON DELETE CASCADE,
          period      TEXT    NOT NULL,   -- 'YYYY-MM'
          metsoa_due  REAL    NOT NULL,
          UNIQUE (account_id, period)
        );
    """)

def seed_sample_account(conn):
    # Ensure S. Nanhoo exists
    owner_id = q(conn, "SELECT id FROM ref_muni_owner WHERE name='S. Nanhoo'").fetchone()[0]
    # Insert your example account if missing
    q(conn, """
        INSERT OR IGNORE INTO bil_muni_account
          (account_number, owner_id, email_electric,
           water_meter_id, elec_meter_id,
           muni_water_meter_no, muni_water_ref,
           muni_elec_meter_no,  muni_elec_ref)
        VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?);
    """, (
        "83327938998",
        owner_id,
        "SouthernMeters@elec.durban.gov.za",
        "CGO310", "W4048914",
        "9027800S", "E9817036"
    ))

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        q(conn, "PRAGMA foreign_keys = ON;")
        q(conn, "BEGIN;")

        ensure_ref_owner(conn)

        # Backup any existing bil_muni_account (data is preserved even if schema is wrong)
        bkp = None
        if table_exists(conn, "bil_muni_account"):
            bkp = backup_table(conn, "bil_muni_account")

        create_new_accounts_schema(conn)
        migrate_old_accounts(conn, bkp)
        swap_in_new_accounts(conn)

        ensure_monthly_tables(conn)
        recreate_core_views(conn)

        seed_sample_account(conn)

        q(conn, "COMMIT;")
        print("✅ Migration complete.")
    except Exception as e:
        q(conn, "ROLLBACK;")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
