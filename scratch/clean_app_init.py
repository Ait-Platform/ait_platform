import re

with open('app/__init__.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Remove the db.create_all() and ALTER TABLE hacks
bad_block_1 = r"""        db\.create_all\(\)
        try:
            db\.session\.execute\(text\("ALTER TABLE mech_job_cards ADD COLUMN mileage VARCHAR\(50\);"\)\)
            db\.session\.commit\(\)
        except Exception:
            db\.session\.rollback\(\)"""

bad_block_2 = r"""        db\.create_all\(\)"""

text = re.sub(bad_block_1, "", text)
text = re.sub(bad_block_2, "", text)

with open('app/__init__.py', 'w', encoding='utf-8') as f:
    f.write(text)
