"""Reusable trade work-item definitions; no shop prices or database writes."""
BOQ_WORK_ITEMS = {
    'carpentry': (
        ('CAR-001', 'Supply and install wooden door', 'Each'),
        ('CAR-002', 'Install door frame', 'Each'),
        ('CAR-003', 'Install door lock', 'Each'),
        ('CAR-004', 'Install skirting', 'm'),
        ('CAR-005', 'Install cornice', 'm'),
        ('CAR-006', 'Construct built-in cupboard', 'm\u00b2'),
        ('CAR-007', 'Install kitchen cabinets', 'm'),
        ('CAR-008', 'Install kitchen countertop', 'm'),
        ('CAR-009', 'Install shelving', 'm'),
        ('CAR-010', 'Repair wooden door', 'Each'),
        ('CAR-011', 'Install timber ceiling', 'm\u00b2'),
        ('CAR-012', 'Install timber flooring', 'm\u00b2'),
        ('CAR-013', 'Construct timber partition', 'm\u00b2'),
        ('CAR-014', 'Install wooden staircase', 'Each'),
        ('CAR-015', 'General carpentry work', 'Hour'),
    ),
}
BOQ_CODE_PREFIXES = {'carpentry': 'CAR', 'boq': 'BOQ'}
CONSTRUCTION_TRADES = {'carpentry':'CAR','plumbing':'PLUM','electrical':'ELEC','painting':'PAINT','building':'BUILD'}


def work_items_for(trade):
    return [dict(code=code, description=description, unit=unit)
            for code, description, unit in BOQ_WORK_ITEMS.get(trade, ())]


def custom_boq_code(trade):
    from uuid import uuid4
    return f'{BOQ_CODE_PREFIXES[trade]}-C-{uuid4().hex.upper()}'


def quotation_catalogue(user_id, shop, item_model, shop_model, part_model=None):
    """Read existing owned trade catalogues; never copy or change their prices."""
    from types import SimpleNamespace
    items, catalogues = [], []
    # Each additional supported trade can use the same adapter and selector.
    for trade,prefix in CONSTRUCTION_TRADES.items():
        sources = shop_model.query.filter_by(user_id=user_id, trade=trade).order_by(shop_model.id).all()
        for source in sources or [None]:
            key = f'{trade}:{source.id}' if source else trade
            catalogues.append(dict(key=key, label=trade.title() + (f' — {source.business_name}' if source else '')))
            saved = item_model.query.filter_by(user_id=user_id, shop_id=source.id, trade=trade).all() if source else []
            by_code = {item.code:item for item in saved}
            definitions = work_items_for(trade)
            definitions += [dict(code=item.code, description=item.description, unit=item.unit) for item in saved if item.code not in {row['code'] for row in definitions}]
            for definition in definitions:
                item = by_code.get(definition['code'])
                items.append(SimpleNamespace(id=item.id if item else f'template:{key}:{definition["code"]}',
                    code=definition['code'], description=item.description if item else definition['description'],
                    unit=item.unit if item else definition['unit'], material_rate=item.material_rate if item else None,
                    labour_rate=item.labour_rate if item else None, catalogue_key=key,catalogue_trade=trade))
            if source and part_model is not None:
                # Generic construction price lists already store a combined unit rate.
                # Read them directly; no catalogue rows or prices are copied/changed.
                from decimal import Decimal
                for part in part_model.query.filter_by(user_id=user_id,shop_id=source.id,trade=trade).order_by(part_model.id).all():
                    items.append(SimpleNamespace(id=f'part:{part.id}',code=getattr(part,'item_code',None) or f'{prefix}-CAT-{part.id}',
                        description=part.part_name,unit='Each',material_rate=Decimal(str(part.default_price or 0)),
                        labour_rate=Decimal('0'),catalogue_key=key,catalogue_trade=trade,rate_basis='combined'))
    own = item_model.query.filter_by(user_id=user_id, shop_id=shop.id, trade='boq').all()
    if own:
        catalogues.append(dict(key='boq', label='BOQ custom items'))
        items.extend(SimpleNamespace(id=item.id,code=item.code,description=item.description,unit=item.unit,
            material_rate=item.material_rate,labour_rate=item.labour_rate,catalogue_key='boq',catalogue_trade='boq') for item in own)
    return items, catalogues
