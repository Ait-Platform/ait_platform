with open("app/models/uip_governance.py", "r", encoding="utf-8") as f:
    text = f.read()

# Remove it from the top
bad_block = """
class UipOrganogramSeat(db.Model):
    __tablename__ = "uip_organogram_seat"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    title = db.Column(db.String(100), nullable=False)
    group_level = db.Column(db.String(50), nullable=False) # CORE_EXCO, SECOND_GROUP
    qualifier = db.Column(db.String(50), nullable=False, default="Voluntary")
    display_order = db.Column(db.Integer, default=0)

"""
if text.startswith(bad_block):
    text = text[len(bad_block):]
    
    # Insert it after "from app.extensions import db"
    import_stmt = "from app.extensions import db\n"
    if import_stmt in text:
        text = text.replace(import_stmt, import_stmt + bad_block)
    else:
        text = import_stmt + bad_block + text

with open("app/models/uip_governance.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed db import error")
