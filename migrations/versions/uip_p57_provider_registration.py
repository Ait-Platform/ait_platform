"""Provider registration marketplace account table

Revision ID: uip_p57
Revises: uip_p56
Create Date: 2026-09-25 20:16:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'uip_p57'
down_revision = 'uip_p56_submembership'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('uip_provider_account',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('provider_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('is_admin', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['provider_id'], ['uip_provider.id'], ),
        sa.ForeignKeyConstraint(['user_id'], ['user.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('provider_id', 'user_id', name='uq_uip_provider_account_user')
    )

def downgrade():
    op.drop_table('uip_provider_account')
