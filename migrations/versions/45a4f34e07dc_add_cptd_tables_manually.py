"""Add CPTD tables manually

Revision ID: 45a4f34e07dc
Revises: 19788357b9a5
Create Date: 2026-08-14 13:24:11.100449

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '45a4f34e07dc'
down_revision: Union[str, Sequence[str], None] = '19788357b9a5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade():
    op.create_table('cptd_evaluations',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('programme', sa.String(length=50), nullable=False),
    sa.Column('rating_programme', sa.Integer(), nullable=True),
    sa.Column('rating_facilitator', sa.Integer(), nullable=True),
    sa.Column('rating_platform', sa.Integer(), nullable=True),
    sa.Column('feedback_text', sa.Text(), nullable=True),
    sa.Column('submitted_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['user_id'], ['user.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('cptd_registrations',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('programme', sa.String(length=50), nullable=False),
    sa.Column('workshop_date', sa.Date(), nullable=True),
    sa.Column('status', sa.String(length=20), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['user_id'], ['user.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('cptd_progress',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('programme', sa.String(length=50), nullable=False),
    sa.Column('module_id', sa.String(length=50), nullable=False),
    sa.Column('status', sa.String(length=20), nullable=True),
    sa.Column('completed_at', sa.DateTime(), nullable=True),
    sa.Column('evidence_data', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['user_id'], ['user.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )

def downgrade():
    op.drop_table('cptd_progress')
    op.drop_table('cptd_registrations')
    op.drop_table('cptd_evaluations')
