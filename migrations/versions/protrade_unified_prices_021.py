"""Canonical client/category construction pricing; preserve all source and financial rows."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_unified_prices_021'
# Pricing is independent of the held local-folder/email migration 020.
down_revision = 'protrade_trade_codes_019'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run online on verified PostgreSQL')
    if not all(bind.execute(sa.text('SELECT current_database(),current_user')).one()):
        raise RuntimeError('Cannot verify database identity')
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute('SET LOCAL extra_float_digits = 3')
    op.execute('LOCK TABLE protrade_boq_items,mech_catalog_parts,mech_shops IN SHARE ROW EXCLUSIVE MODE')
    op.add_column('protrade_boq_items',sa.Column('combined_rate',sa.Numeric()))
    op.add_column('protrade_boq_items',sa.Column('legacy_part_id',sa.Integer(),sa.ForeignKey('mech_catalog_parts.id')))
    op.create_unique_constraint('uq_protrade_legacy_part','protrade_boq_items',['legacy_part_id'])
    for field in ('material_rate','labour_rate'):
        op.alter_column('protrade_boq_items',field,existing_type=sa.Numeric(14,2),nullable=True)
    # Generated columns are replaced; every existing rate/amount computes identically.
    op.drop_column('protrade_boq_items','total_amount')
    op.drop_column('protrade_boq_items','total_rate')
    op.add_column('protrade_boq_items',sa.Column('total_rate',sa.Numeric(),sa.Computed('coalesce(combined_rate,material_rate+labour_rate)',persisted=True)))
    op.add_column('protrade_boq_items',sa.Column('total_amount',sa.Numeric(28,2),sa.Computed('round(quantity*coalesce(combined_rate,material_rate+labour_rate),2)',persisted=True)))
    op.create_check_constraint('ck_protrade_price_basis','protrade_boq_items',
        '(combined_rate IS NOT NULL AND combined_rate >= 0 AND combined_rate <= 999999999999.99 AND material_rate IS NULL AND labour_rate IS NULL) OR (combined_rate IS NULL AND material_rate IS NOT NULL AND labour_rate IS NOT NULL)')
    # Fail closed on ambiguous ownership or invalid prices, rather than skip or invent data.
    invalid = bind.execute(sa.text("""SELECT p.id FROM mech_catalog_parts p LEFT JOIN mech_shops s ON s.id=p.shop_id
        WHERE p.trade IN ('carpentry','plumbing','electrical','painting','building','general','boq')
        AND (s.id IS NULL OR p.user_id IS DISTINCT FROM s.user_id OR p.default_price IS NULL
             OR NOT (p.default_price >= 0 AND p.default_price <= 999999999999.99)) LIMIT 1""")).first()
    if invalid:
        raise RuntimeError('Reconcile construction catalogue ownership/price before migration: item '+str(invalid[0]))
    # Original rows and IDs are retained as read-only provenance. No material/labour allocation.
    op.execute("""INSERT INTO protrade_boq_items
        (user_id,shop_id,trade,code,description,unit,material_rate,labour_rate,combined_rate,legacy_part_id,quantity,created_at)
        SELECT p.user_id,p.shop_id,p.trade,
        coalesce(nullif(p.item_code,''),
            CASE p.trade WHEN 'carpentry' THEN 'CAR' WHEN 'plumbing' THEN 'PLUM' WHEN 'electrical' THEN 'ELEC'
            WHEN 'painting' THEN 'PAINT' WHEN 'building' THEN 'BUILD' WHEN 'general' THEN 'GEN' ELSE 'BOQ' END || '-CAT-' || p.id),
        p.part_name,'Each',NULL,NULL,p.default_price::text::numeric,p.id,1,p.created_at
        FROM mech_catalog_parts p JOIN mech_shops s ON s.id=p.shop_id AND s.user_id=p.user_id
        WHERE p.trade IN ('carpentry','plumbing','electrical','painting','building','general','boq')
        ORDER BY p.id""")
    # Duplicate codes abort the transaction for reconciliation; never silently merge records.


def downgrade():
    raise RuntimeError('Forward-only: preserve canonical and original catalogue prices')
