"""Set show_on_welcome true for mechanic and practice_crm

Revision ID: 8c6a21489a8c
Revises: 96b55415fafd
Create Date: 2026-08-02 15:42:51.682497

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8c6a21489a8c'
down_revision: Union[str, Sequence[str], None] = '96b55415fafd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "UPDATE auth_subject SET show_on_welcome = true WHERE slug IN ('mechanic', 'practice_crm');"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE auth_subject SET show_on_welcome = false WHERE slug IN ('mechanic', 'practice_crm');"
    )
