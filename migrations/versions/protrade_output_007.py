"""Durable Output email request receipts; no document or accounting changes."""
from alembic import op
import sqlalchemy as sa

revision='protrade_output_007'
down_revision='protrade_debtor_006'
branch_labels=None
depends_on=None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql':
        raise RuntimeError('ProTrade Output requires PostgreSQL')
    if not op.get_context().as_sql:
        if not all(bind.execute(sa.text('SELECT current_database(), current_user')).one()):
            raise RuntimeError('Cannot verify database identity')
    op.create_table('protrade_output_email',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('request_key',sa.String(64),nullable=False,unique=True),
        sa.Column('user_id',sa.Integer(),sa.ForeignKey('user.id'),nullable=False),
        sa.Column('shop_id',sa.Integer(),sa.ForeignKey('mech_shops.id'),nullable=False),
        sa.Column('debtor_id',sa.Integer(),sa.ForeignKey('debtor.id'),nullable=False),
        sa.Column('selection',sa.String(100),nullable=False),
        sa.Column('recipient',sa.String(150),nullable=False),
        sa.Column('status',sa.String(30),nullable=False),
        sa.Column('created_at',sa.DateTime(),nullable=False))


def downgrade():
    raise RuntimeError('Destructive downgrade disabled: preserve email duplicate-prevention receipts')
