"""Append-only client work history using existing shops and debtors."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_work_activity_015'
down_revision = 'protrade_document_tokens_013'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run work activity migration online on PostgreSQL')
    op.create_table('protrade_work_activity',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('user.id'), nullable=False),
        sa.Column('shop_id', sa.Integer(), sa.ForeignKey('mech_shops.id'), nullable=False),
        sa.Column('debtor_id', sa.Integer(), sa.ForeignKey('debtor.id'), nullable=False),
        sa.Column('entry_key', sa.String(32), nullable=False),
        sa.Column('revision', sa.Integer(), nullable=False),
        sa.Column('previous_id', sa.Integer(), sa.ForeignKey('protrade_work_activity.id'), unique=True),
        sa.Column('work_date', sa.Date(), nullable=False),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('recorded_at', sa.DateTime(), nullable=False, server_default=sa.text("(now() AT TIME ZONE 'UTC')")),
        sa.UniqueConstraint('user_id', 'entry_key', 'revision', name='uq_protrade_work_revision'),
        sa.CheckConstraint('revision > 0 AND ((revision=1 AND previous_id IS NULL) OR (revision>1 AND previous_id IS NOT NULL))', name='ck_protrade_work_revision'),
        sa.CheckConstraint('length(trim(description)) BETWEEN 1 AND 4000', name='ck_protrade_work_description'))
    schema = bind.execute(sa.text('SELECT current_schema()')).scalar_one()
    quoted = bind.dialect.identifier_preparer.quote_schema(schema)
    op.execute(f"""CREATE FUNCTION {quoted}.protrade_guard_work_activity() RETURNS trigger
        LANGUAGE plpgsql SET search_path={quoted},pg_catalog AS $$ BEGIN
        IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Work activity history is append-only'; END IF;
        IF NOT EXISTS(SELECT 1 FROM debtor d JOIN mech_shops s ON s.id=d.shop_id
            WHERE d.id=NEW.debtor_id AND d.shop_id=NEW.shop_id AND d.user_id=NEW.user_id AND s.user_id=NEW.user_id) THEN
            RAISE EXCEPTION 'Invalid work activity account'; END IF;
        IF NEW.previous_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM protrade_work_activity a
            WHERE a.id=NEW.previous_id AND a.user_id=NEW.user_id AND a.shop_id=NEW.shop_id
            AND a.debtor_id=NEW.debtor_id AND a.entry_key=NEW.entry_key AND a.revision+1=NEW.revision) THEN
            RAISE EXCEPTION 'Invalid work activity correction'; END IF;
        RETURN NEW; END $$""")
    op.execute(f'CREATE TRIGGER protrade_work_history BEFORE INSERT OR UPDATE OR DELETE ON protrade_work_activity FOR EACH ROW EXECUTE FUNCTION {quoted}.protrade_guard_work_activity()')


def downgrade():
    raise RuntimeError('Preserve client work audit history')
