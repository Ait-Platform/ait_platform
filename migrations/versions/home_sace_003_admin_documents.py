"""Separate admin-upload approval provenance; retain immutable HOME history."""
from alembic import op
import sqlalchemy as sa

revision = 'home_sace_003'
down_revision = 'home_sace_002'
branch_labels = None
depends_on = None


def immutable_function():
    bind = op.get_bind()
    schema = bind.execute(sa.text('SELECT current_schema()')).scalar_one()
    return bind.dialect.identifier_preparer.quote_schema(schema) + '.home_document_version_immutable'


def upgrade():
    op.add_column('sace_home_document_version', sa.Column('approved_by_admin_user_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_home_document_admin_user', 'sace_home_document_version', 'user',
        ['approved_by_admin_user_id'], ['id'], ondelete='RESTRICT')
    op.alter_column('sace_home_document_version', 'approved_by', existing_type=sa.Integer(), nullable=True)
    op.create_check_constraint('ck_home_document_approver', 'sace_home_document_version',
        '(approved_by IS NOT NULL AND approved_by_admin_user_id IS NULL) OR '
        '(approved_by IS NULL AND approved_by_admin_user_id IS NOT NULL)')
    function = immutable_function()
    op.execute(f"""CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'HOME document version history is append-only'; END $$""")
    op.execute('CREATE TRIGGER home_document_version_immutable BEFORE UPDATE OR DELETE ON '
        f'sace_home_document_version FOR EACH ROW EXECUTE FUNCTION {function}()')


def downgrade():
    if op.get_bind().execute(sa.text('SELECT count(*) FROM sace_home_document_version '
            'WHERE approved_by_admin_user_id IS NOT NULL')).scalar_one():
        raise RuntimeError('Preserve admin publication history; use forward recovery.')
    op.execute('DROP TRIGGER home_document_version_immutable ON sace_home_document_version')
    op.execute('DROP FUNCTION ' + immutable_function() + '()')
    op.drop_constraint('ck_home_document_approver', 'sace_home_document_version', type_='check')
    op.alter_column('sace_home_document_version', 'approved_by', existing_type=sa.Integer(), nullable=False)
    op.drop_constraint('fk_home_document_admin_user', 'sace_home_document_version', type_='foreignkey')
    op.drop_column('sace_home_document_version', 'approved_by_admin_user_id')
