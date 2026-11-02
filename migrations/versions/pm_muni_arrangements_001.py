"""Account-linked municipal arrangements, payment evidence and append-only audit."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg
revision='pm_muni_arrangements_001'
down_revision='pm_tariff_authority_002'
branch_labels=None
depends_on=None


def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.create_table('bil_municipal_arrangement',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('property_id',sa.Integer(),sa.ForeignKey('bil_property.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('account_id',sa.Integer(),sa.ForeignKey('bil_muni_account.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('reference',sa.String(150),nullable=False),sa.Column('reference_key',sa.String(150),nullable=False),
        sa.Column('agreement_date',sa.Date(),nullable=False),sa.Column('original_amount',sa.Numeric(18,2),nullable=False),
        sa.Column('instalment_amount',sa.Numeric(18,2),nullable=False),sa.Column('instalments',sa.Integer(),nullable=False),
        sa.Column('frequency',sa.String(20),nullable=False),sa.Column('first_payment_date',sa.Date(),nullable=False),
        sa.Column('status',sa.String(20),nullable=False,server_default='active'),sa.Column('document_reference',sa.String(1000)),
        sa.Column('version',sa.Integer(),nullable=False,server_default='1'),
        sa.Column('created_by',sa.Integer(),sa.ForeignKey('user.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('modified_by',sa.Integer(),sa.ForeignKey('user.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('created_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.Column('modified_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.UniqueConstraint('account_id','reference_key',name='uq_muni_arrangement_reference'),
        sa.CheckConstraint("original_amount>0 AND instalment_amount>0 AND instalments BETWEEN 1 AND 600 AND original_amount>instalment_amount*(instalments-1) AND original_amount<=instalment_amount*instalments",name='ck_muni_arrangement_amounts'),
        sa.CheckConstraint("frequency IN ('weekly','fortnightly','monthly','quarterly') AND status IN ('active','on_hold','cancelled') AND first_payment_date>=agreement_date AND version>0 AND length(btrim(reference))>0 AND length(btrim(reference_key))>0",name='ck_muni_arrangement_terms'))
    op.create_table('bil_municipal_arrangement_payment',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('arrangement_id',sa.Integer(),sa.ForeignKey('bil_municipal_arrangement.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('account_id',sa.Integer(),sa.ForeignKey('bil_muni_account.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('source_payment_id',sa.Integer(),sa.ForeignKey('bil_muni_payment.id',ondelete='RESTRICT'),unique=True),
        sa.Column('payment_date',sa.Date(),nullable=False),sa.Column('amount',sa.Numeric(18,2),nullable=False),
        sa.Column('reference',sa.String(150),nullable=False),sa.Column('reference_key',sa.String(150),nullable=False),
        sa.Column('recorded_by',sa.Integer(),sa.ForeignKey('user.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('recorded_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.UniqueConstraint('account_id','reference_key',name='uq_muni_arrangement_payment_reference'),
        sa.CheckConstraint('amount>0 AND length(btrim(reference))>0',name='ck_muni_arrangement_payment_amount'))
    op.create_table('bil_municipal_arrangement_event',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('arrangement_id',sa.Integer(),sa.ForeignKey('bil_municipal_arrangement.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('kind',sa.String(20),nullable=False),sa.Column('actor_id',sa.Integer(),sa.ForeignKey('user.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('request_key',pg.UUID(as_uuid=False),nullable=False,unique=True),sa.Column('payload_hash',sa.String(64),nullable=False),
        sa.Column('before_data',pg.JSONB()),sa.Column('after_data',pg.JSONB(),nullable=False),sa.Column('note',sa.String(5000)),
        sa.Column('recorded_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()))


def downgrade():raise RuntimeError('Forward-only: preserve arrangement terms, payments and audit history.')
