"""Stable construction codes on existing price-list rows; no historical document changes."""
import re
from alembic import op
import sqlalchemy as sa

revision='protrade_trade_codes_019'
down_revision='protrade_vat_invoices_018'
branch_labels=None
depends_on=None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run construction item-code migration online on PostgreSQL')
    op.execute('LOCK TABLE mech_catalog_parts,mech_shops IN SHARE ROW EXCLUSIVE MODE')
    op.add_column('mech_catalog_parts',sa.Column('item_code',sa.String(50)))
    for trade,prefix in {'carpentry':'CAR','plumbing':'PLUM','electrical':'ELEC','painting':'PAINT','building':'BUILD'}.items():
        rows=bind.execute(sa.text('''SELECT p.id,p.shop_id FROM mech_catalog_parts p JOIN mech_shops s
            ON s.id=p.shop_id AND s.user_id=p.user_id AND s.trade=p.trade WHERE p.trade=:trade ORDER BY p.shop_id,p.id'''),dict(trade=trade)).all()
        last={}
        for identifier,shop_id in rows:
            if shop_id not in last:
                codes=bind.execute(sa.text('''SELECT code FROM protrade_boq_items WHERE shop_id=:shop
                    UNION ALL SELECT line->>'code' FROM protrade_boq_quotes q,
                    LATERAL jsonb_array_elements(q.items::jsonb) line WHERE q.shop_id=:shop'''),dict(shop=shop_id)).scalars().all()
                last[shop_id]=max([int(match.group(1)) for code in codes if (match:=re.fullmatch(re.escape(prefix)+r'-(\d+)',code or ''))]+[15 if trade=='carpentry' else 0])
            last[shop_id]+=1
            bind.execute(sa.text('UPDATE mech_catalog_parts SET item_code=:code WHERE id=:id'),dict(code=f'{prefix}-{last[shop_id]:03d}',id=identifier))
    op.create_unique_constraint('uq_protrade_catalogue_code','mech_catalog_parts',['shop_id','trade','item_code'])


def downgrade():
    raise RuntimeError('Preserve published catalogue codes and quotation references')
