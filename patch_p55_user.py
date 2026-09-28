import sys
import re
with open("migrations/versions/uip_p55_sub_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    "op.add_column('uip_proposal', sa.Column('originating_subcommittee_id', sa.Integer(), nullable=True))",
    "op.add_column('uip_proposal', sa.Column('originating_subcommittee_id', sa.Integer(), nullable=True))\n    op.add_column('uip_committee_member', sa.Column('user_id', sa.Integer(), nullable=True))\n    op.create_foreign_key('fk_uip_committee_member_user', 'uip_committee_member', 'user', ['user_id'], ['id'])"
)

c = c.replace(
    "op.drop_constraint('fk_uip_proposal_subcommittee_org', 'uip_proposal', type_='foreignkey')",
    "op.drop_constraint('fk_uip_proposal_subcommittee_org', 'uip_proposal', type_='foreignkey')\n    op.drop_constraint('fk_uip_committee_member_user', 'uip_committee_member', type_='foreignkey')\n    op.drop_column('uip_committee_member', 'user_id')"
)

with open("migrations/versions/uip_p55_sub_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
