import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """        # New Digital Room upgrades
        db.session.execute(text("ALTER TABLE uip_resolution ADD COLUMN voting_scope VARCHAR(20) DEFAULT 'EXCO';"))
        db.session.execute(text("ALTER TABLE uip_resolution ADD COLUMN quorum_target INTEGER DEFAULT 50;"))
        db.session.execute(text("ALTER TABLE uip_resolution ADD COLUMN expires_at TIMESTAMP;"))"""

new_block = """        # New Digital Room upgrades
        for col_sql in [
            "ALTER TABLE uip_resolution ADD COLUMN voting_scope VARCHAR(20) DEFAULT 'EXCO';",
            "ALTER TABLE uip_resolution ADD COLUMN quorum_target INTEGER DEFAULT 50;",
            "ALTER TABLE uip_resolution ADD COLUMN expires_at TIMESTAMP;"
        ]:
            try:
                db.session.execute(text(col_sql))
            except Exception:
                db.session.rollback()"""

text = text.replace(old_block, new_block)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated dev_upgrade_db robustness")
