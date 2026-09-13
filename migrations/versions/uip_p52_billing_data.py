"""UIP set billing_scope organization

Revision ID: uip_p52_billing_data
Revises: uip_p51_billing
Create Date: 2026-09-13 08:35:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


# revision identifiers, used by Alembic.
revision = 'uip_p52_billing_data'
down_revision = 'uip_p51_billing'
branch_labels = None
depends_on = None

def upgrade():
    op.execute("UPDATE auth_subject SET billing_scope = 'organization' WHERE slug = 'uip'")

def downgrade():
    op.execute("UPDATE auth_subject SET billing_scope = 'user' WHERE slug = 'uip'")
