from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'add_letterhead_to_sender'
down_revision = 'a1b2c3d4e5f6'  # Assuming this is the latest
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('sender_profile', sa.Column('letterhead_url', sa.String(length=255), nullable=True))
    op.add_column('sender_profile', sa.Column('use_custom_letterhead', sa.Boolean(), nullable=True, server_default='false'))


def downgrade():
    op.drop_column('sender_profile', 'use_custom_letterhead')
    op.drop_column('sender_profile', 'letterhead_url')
