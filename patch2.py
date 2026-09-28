import sys
with open("app/models/uip_governance.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'db.ForeignKeyConstraint(["establishing_resolution_id", "organization_id"]',
    'db.UniqueConstraint("id", "organization_id", name="uq_uip_subcommittee_org"),\n        db.ForeignKeyConstraint(["establishing_resolution_id", "organization_id"]'
)

with open("app/models/uip_governance.py", "w", encoding="utf-8") as f:
    f.write(c)

with open("migrations/versions/uip_p55_sub_tools.py", "r", encoding="utf-8") as f:
    c2 = f.read()

c2 = c2.replace(
    "op.add_column('uip_proposal', sa.Column('originating_subcommittee_id', sa.Integer(), nullable=True))",
    "op.create_unique_constraint('uq_uip_subcommittee_org', 'uip_subcommittee', ['id', 'organization_id'])\n    op.add_column('uip_proposal', sa.Column('originating_subcommittee_id', sa.Integer(), nullable=True))"
)
c2 = c2.replace(
    "op.drop_constraint('fk_uip_proposal_subcommittee_org', 'uip_proposal', type_='foreignkey')",
    "op.drop_constraint('fk_uip_proposal_subcommittee_org', 'uip_proposal', type_='foreignkey')\n    op.drop_constraint('uq_uip_subcommittee_org', 'uip_subcommittee', type_='unique')"
)

with open("migrations/versions/uip_p55_sub_tools.py", "w", encoding="utf-8") as f:
    f.write(c2)
print("Updated model and migration")
