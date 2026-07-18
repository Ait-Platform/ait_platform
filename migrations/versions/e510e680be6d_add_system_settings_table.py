"""Add system_settings table

Revision ID: e510e680be6d
Revises: bec556b95366
Create Date: 2026-07-18 16:53:17.318153

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e510e680be6d'
down_revision: Union[str, Sequence[str], None] = 'bec556b95366'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
        CREATE TABLE IF NOT EXISTS system_settings (
            key VARCHAR(255) PRIMARY KEY,
            value VARCHAR(255) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("INSERT INTO system_settings (key, value) VALUES ('mechanic_quote_cents', '500') ON CONFLICT DO NOTHING")
    op.execute("INSERT INTO system_settings (key, value) VALUES ('mechanic_invoice_cents', '1000') ON CONFLICT DO NOTHING")


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP TABLE IF EXISTS system_settings")
