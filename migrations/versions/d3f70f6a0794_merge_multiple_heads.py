"""Merge multiple heads

Revision ID: d3f70f6a0794
Revises: 7da57fffdba9, b3d537f90c43, uip_p11_completion
Create Date: 2026-09-12 14:56:45.496096

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd3f70f6a0794'
down_revision: Union[str, Sequence[str], None] = ('7da57fffdba9', 'b3d537f90c43', 'uip_p11_completion')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
