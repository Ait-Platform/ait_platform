import re
with open("app/models/uip_governance.py", "r", encoding="utf-8") as f:
    c = f.read()

# Add UNIQUE constraint to UipSubcommittee
if 'db.UniqueConstraint("id", "organization_id", name="uq_uip_subcommittee_org")' not in c:
    c = c.replace(
        """class UipSubcommittee(db.Model):
    __tablename__ = "uip_subcommittee"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)""",
        """class UipSubcommittee(db.Model):
    __tablename__ = "uip_subcommittee"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_subcommittee_org"),"""
    )
    # Fix the trailing comma in the replace if needed by replacing the previous __table_args__ definition.
    # Actually wait, UipSubcommittee already HAS __table_args__!
    c = c.replace(
        "    __table_args__ = (\n        db.ForeignKeyConstraint",
        "    __table_args__ = (\n        db.UniqueConstraint(\"id\", \"organization_id\", name=\"uq_uip_subcommittee_org\"),\n        db.ForeignKeyConstraint"
    )
    # Undo the accidental double insertion if it happened
    c = c.replace("    __table_args__ = (\n        db.UniqueConstraint(\"id\", \"organization_id\", name=\"uq_uip_subcommittee_org\"),\n", "", 1) # wait just do it manually cleanly
