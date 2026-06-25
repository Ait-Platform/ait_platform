"""merge

Revision ID: e4991ea413ee
Revises: 2e7b57097f5f, ref_country_currency
Create Date: 2026-06-24 18:37:49.155765

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e4991ea413ee'
down_revision: Union[str, Sequence[str], None] = ('2e7b57097f5f', 'ref_country_currency')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
