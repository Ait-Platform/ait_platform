"""Reading-only 48-hour completion safeguard; no historical data adoption."""
from alembic import op
import sqlalchemy as sa

revision = 'reading_sace_002'
down_revision = 'reading_sace_001'
branch_labels = None
depends_on = None


def upgrade():
    table = 'sace_reading_engagement'
    op.alter_column(table, 'status', existing_type=sa.String(16), type_=sa.String(24))
    op.add_column(table, sa.Column('completion_requested_by_user_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_reading_completion_requester', table, 'user', ['completion_requested_by_user_id'], ['id'], ondelete='RESTRICT')
    op.add_column(table, sa.Column('completion_requested_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column(table, sa.Column('completion_deadline', sa.DateTime(timezone=True), nullable=True))
    for suffix in ('status', 'times', 'actor'):
        op.drop_constraint('ck_reading_engagement_' + suffix, table, type_='check')
    op.create_check_constraint('ck_reading_engagement_status', table, "status IN ('active','completion_pending','completed','revoked')")
    op.create_check_constraint('ck_reading_engagement_times', table, "(status IN ('active','completion_pending') AND completed_at IS NULL AND revoked_at IS NULL) OR (status = 'completed' AND completed_at IS NOT NULL AND revoked_at IS NULL) OR (status = 'revoked' AND revoked_at IS NOT NULL AND completed_at IS NULL)")
    op.create_check_constraint('ck_reading_engagement_actor', table, "status IN ('active','completion_pending') OR ended_by_user_id IS NOT NULL")
    op.create_check_constraint('ck_reading_engagement_pending', table, "(completion_requested_by_user_id IS NULL AND completion_requested_at IS NULL AND completion_deadline IS NULL AND status != 'completion_pending') OR (completion_requested_by_user_id IS NOT NULL AND completion_requested_at IS NOT NULL AND completion_deadline IS NOT NULL AND completion_deadline = completion_requested_at + interval '48 hours' AND status IN ('completion_pending','completed','revoked'))")


def downgrade():
    # Never discard completion requests/deadlines or weaken a pending closure.
    count = op.get_bind().execute(sa.text("SELECT count(*) FROM sace_reading_engagement WHERE completion_requested_at IS NOT NULL OR status = 'completion_pending'")).scalar_one()
    if count:
        raise RuntimeError('Preserve Reading completion history; use forward recovery.')
    table = 'sace_reading_engagement'
    for suffix in ('pending', 'status', 'times', 'actor'):
        op.drop_constraint('ck_reading_engagement_' + suffix, table, type_='check')
    op.create_check_constraint('ck_reading_engagement_status', table, "status IN ('active','completed','revoked')")
    op.create_check_constraint('ck_reading_engagement_times', table, "(status = 'active' AND completed_at IS NULL AND revoked_at IS NULL) OR (status = 'completed' AND completed_at IS NOT NULL AND revoked_at IS NULL) OR (status = 'revoked' AND revoked_at IS NOT NULL AND completed_at IS NULL)")
    op.create_check_constraint('ck_reading_engagement_actor', table, "status = 'active' OR ended_by_user_id IS NOT NULL")
    op.drop_constraint('fk_reading_completion_requester', table, type_='foreignkey')
    for column in ('completion_deadline', 'completion_requested_at', 'completion_requested_by_user_id'):
        op.drop_column(table, column)
    op.alter_column(table, 'status', existing_type=sa.String(24), type_=sa.String(16))
