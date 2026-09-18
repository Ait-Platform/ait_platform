import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_upgrade = """def dev_upgrade_db(org_slug):
    from sqlalchemy import text
    try:
        db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN parent_id INTEGER REFERENCES core_interaction(id);"))
        db.session.commit()
        return "Success"
    except Exception as e:
        db.session.rollback()
        return str(e)"""

new_upgrade = """def dev_upgrade_db(org_slug):
    from sqlalchemy import text
    try:
        # Existing master ticket upgrade
        try:
            db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN parent_id INTEGER REFERENCES core_interaction(id);"))
        except Exception:
            db.session.rollback()
            
        # New Digital Room upgrades
        db.session.execute(text("ALTER TABLE uip_resolution ADD COLUMN voting_scope VARCHAR(20) DEFAULT 'EXCO';"))
        db.session.execute(text("ALTER TABLE uip_resolution ADD COLUMN quorum_target INTEGER DEFAULT 50;"))
        db.session.execute(text("ALTER TABLE uip_resolution ADD COLUMN expires_at TIMESTAMP;"))
        
        db.session.execute(text(\"\"\"
            CREATE TABLE IF NOT EXISTS uip_resolution_vote (
                id SERIAL PRIMARY KEY,
                resolution_id INTEGER NOT NULL REFERENCES uip_resolution(id),
                user_id INTEGER NOT NULL REFERENCES "user"(id),
                vote VARCHAR(20) NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_uip_resolution_vote UNIQUE (resolution_id, user_id)
            );
        \"\"\"))
        
        db.session.execute(text(\"\"\"
            CREATE TABLE IF NOT EXISTS uip_resolution_comment (
                id SERIAL PRIMARY KEY,
                resolution_id INTEGER NOT NULL REFERENCES uip_resolution(id),
                user_id INTEGER NOT NULL REFERENCES "user"(id),
                message TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        \"\"\"))
        
        db.session.commit()
        return "Success: DB Upgraded for Digital Committee Room"
    except Exception as e:
        db.session.rollback()
        return "Error: " + str(e)"""

text = text.replace(old_upgrade, new_upgrade)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated dev_upgrade_db")
