"""Update endpoints and names for practice crm and mechanic

Revision ID: 96b55415fafd
Revises: e510e680be6d
Create Date: 2026-08-02 12:10:52.836603

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '96b55415fafd'
down_revision: Union[str, Sequence[str], None] = 'e510e680be6d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        "UPDATE auth_subject SET name = 'Health Practice CRM', about_endpoint = 'practice_crm_bp.about', pay_endpoint = 'paddle_bp.paddle_start' WHERE slug = 'practice_crm';"
    )
    op.execute(
        "UPDATE auth_subject SET name = 'Home Mechanic CRM', about_endpoint = 'mechanic_bp.about', pay_endpoint = 'paddle_bp.paddle_start' WHERE slug = 'mechanic';"
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(
        "UPDATE auth_subject SET name = 'Practice CRM', about_endpoint = NULL, pay_endpoint = NULL WHERE slug = 'practice_crm';"
    )
    op.execute(
        "UPDATE auth_subject SET name = 'Mechanic CRM', about_endpoint = NULL, pay_endpoint = NULL WHERE slug = 'mechanic';"
    )
