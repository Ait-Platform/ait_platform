"""add status to user_enrollment

Revision ID: 2e7b57097f5f
Revises: 2ae144960bd1
Create Date: 2025-10-19 18:43:11.367568

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2e7b57097f5f'
down_revision = None

#down_revision: Union[str, Sequence[str], None] = '2ae144960bd1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    from alembic import op
    import sqlalchemy as sa
    from sqlalchemy.engine.reflection import Inspector
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    
    columns = [col['name'] for col in inspector.get_columns("user_enrollment")]
    if "status" not in columns:
        op.add_column(
            "user_enrollment",
            sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'active'")),
        )
        op.create_index("idx_user_enrollment_status", "user_enrollment", ["status"], unique=False)

def downgrade():
    from alembic import op
    op.drop_index("idx_user_enrollment_status", table_name="user_enrollment")
    op.drop_column("user_enrollment", "status")
