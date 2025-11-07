# save this as scripts/inspect_overall_table.py and run:  python scripts/inspect_overall_table.py
from sqlalchemy import text

# adapt these imports to your app
from app import create_app, db   # if you don't use a factory, do: from app import app, db

def main():
    try:
        app = create_app()  # if you don't use a factory, delete this line
    except TypeError:
        # already returns an app, ignore
        pass

    # get the Flask app object either from create_app() or from app
    flapp = locals().get("app") if "app" in locals() else globals().get("app")

    with flapp.app_context():
        rows = db.session.execute(text("PRAGMA table_info(lca_overall_item)")).fetchall()
        print("lca_overall_item columns:")
        for r in rows:
            # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            print(f"  {r[1]:<18}  {r[2]:<12}  NOTNULL={r[3]}  DEFAULT={r[4]}  PK={r[5]}")

        # quick sample counts by band/type if those columns exist:
        cols = {r[1] for r in rows}
        if {"band", "type"}.issubset(cols):
            res = db.session.execute(text("""
                SELECT band, type, COUNT(*) AS n
                FROM lca_overall_item
                GROUP BY band, type
                ORDER BY band, type
            """)).fetchall()
            print("\nCounts by band/type:")
            for band, typ, n in res:
                print(f"  {band:<6} {typ:<10}  {n}")
        else:
            print("\n(‘band’ and/or ‘type’ not present; skip counts.)")

if __name__ == "__main__":
    main()
