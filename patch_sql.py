with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """        for col_sql in [
            "ALTER TABLE uip_resolution ADD COLUMN voting_scope VARCHAR(20) DEFAULT 'EXCO';",
            "ALTER TABLE uip_resolution ADD COLUMN quorum_target INTEGER DEFAULT 50;",
            "ALTER TABLE uip_resolution ADD COLUMN expires_at TIMESTAMP;"
        ]:"""

new_logic = """        for col_sql in [
            "ALTER TABLE uip_resolution ADD COLUMN voting_scope VARCHAR(20) DEFAULT 'EXCO';",
            "ALTER TABLE uip_resolution ADD COLUMN quorum_target INTEGER DEFAULT 50;",
            "ALTER TABLE uip_resolution ADD COLUMN expires_at TIMESTAMP;",
            "ALTER TABLE uip_organogram_seat ADD COLUMN duty VARCHAR(50) DEFAULT 'committee_member';"
        ]:"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added SQL migration to dev_upgrade_db")
