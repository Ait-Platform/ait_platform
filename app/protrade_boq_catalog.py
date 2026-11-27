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
# Definitions carry no selling prices. Extend this registry for another construction trade.
CONSTRUCTION_TRADES = {'carpentry':'CAR','plumbing':'PLUM','electrical':'ELEC','painting':'PAINT','building':'BUILD','general':'GEN'}
CATEGORY_LABELS = {key: key.title() for key in CONSTRUCTION_TRADES}
CATEGORY_LABELS['general'] = 'General Works'
BOQ_WORK_ITEMS.update({
    'plumbing': (
        ('PLUM-STD-001','Repair leaking pipe','m'),
        ('PLUM-STD-002','Supply and install water pipe','m'),
        ('PLUM-STD-003','Supply and install tap','Each'),
        ('PLUM-STD-004','Supply and install wash basin','Each'),
        ('PLUM-STD-005','Supply and install toilet','Each'),
        ('PLUM-STD-006','General plumbing work','Hour'),
    ),
    'electrical': (
        ('ELEC-STD-001','Supply and install electrical conduit','m'),
        ('ELEC-STD-002','Supply and install electrical cable','m'),
        ('ELEC-STD-003','Supply and install socket outlet','Each'),
        ('ELEC-STD-004','Supply and install light fitting','Each'),
        ('ELEC-STD-005','Supply and install light switch','Each'),
        ('ELEC-STD-006','General electrical work','Hour'),
    ),
    'painting': (
        ('PAINT-STD-001','Prepare wall surface for painting','m\u00b2'),
        ('PAINT-STD-002','Apply wall primer','m\u00b2'),
        ('PAINT-STD-003','Paint internal walls','m\u00b2'),
        ('PAINT-STD-004','Paint external walls','m\u00b2'),
        ('PAINT-STD-005','Paint ceiling','m\u00b2'),
        ('PAINT-STD-006','Paint door','Each'),
        ('PAINT-STD-007','General painting work','Hour'),
    ),
    'building': (
        ('BUILD-STD-001','Excavate foundations','m\u00b3'),
        ('BUILD-STD-002','Place concrete','m\u00b3'),
        ('BUILD-STD-003','Build brick wall','m\u00b2'),
        ('BUILD-STD-004','Plaster wall','m\u00b2'),
        ('BUILD-STD-005','Lay floor screed','m\u00b2'),
        ('BUILD-STD-006','General building work','Hour'),
    ),
    'general': (
        ('GEN-001','General works','Hour'),
        ('GEN-002','Site preparation','m\u00b2'),
        ('GEN-003','Remove rubble','m\u00b3'),
        ('GEN-004','Clean work area','m\u00b2'),
    ),
})
# All registered profiles consume the same interface and shop-owned rate store.
TRADE_CATALOGUES = dict(CONSTRUCTION_TRADES, mechanic='MECH', panel='PANEL', auto_elec='AUTO', generic='WORK', protrade='PRO', boq='BOQ')
CATEGORY_LABELS.update(mechanic='General Mechanic', panel='Panel Beater', auto_elec='Auto Electrician', generic='Generic Business', protrade='ProTrade Default', boq='Bill of Quantities')
BOQ_WORK_ITEMS.update({
    'mechanic': (
        ('MECH-STD-001','Vehicle diagnostic inspection','Hour'),
        ('MECH-STD-002','Replace engine oil and filter','Each'),
        ('MECH-STD-003','Replace brake pads','Each'),
        ('MECH-STD-004','Replace spark plugs','Each'),
        ('MECH-STD-005','General mechanical work','Hour'),
    ),
    'auto_elec': (
        ('AUTO-STD-001','Vehicle electrical diagnostic inspection','Hour'),
        ('AUTO-STD-002','Test battery and charging system','Each'),
        ('AUTO-STD-003','Supply and replace vehicle battery','Each'),
        ('AUTO-STD-004','Repair starter motor','Each'),
        ('AUTO-STD-005','Repair alternator','Each'),
        ('AUTO-STD-006','Repair vehicle wiring harness','Hour'),
        ('AUTO-STD-007','Supply and replace vehicle light','Each'),
        ('AUTO-STD-008','General auto electrical work','Hour'),
    ),
    'panel': (
        ('PANEL-STD-001','Inspect vehicle body damage','Each'),
        ('PANEL-STD-002','Repair damaged body panel','Each'),
        ('PANEL-STD-003','Supply and replace body panel','Each'),
        ('PANEL-STD-004','Prepare and refinish body panel','Each'),
        ('PANEL-STD-005','General panel beating work','Hour'),
    ),
    'generic': (
        ('WORK-STD-001','General service work','Hour'),
        ('WORK-STD-002','Supply and install client item','Each'),
        ('WORK-STD-003','Inspect and assess work','Each'),
    ),
    'protrade': (
        ('PRO-STD-001','General trade work','Hour'),
        ('PRO-STD-002','Supply and install trade item','Each'),
        ('PRO-STD-003','Inspect and assess trade work','Each'),
    ),
    'boq': (('BOQ-STD-001','Measured work allowance','Each'),),
})
BOQ_CODE_PREFIXES = dict(TRADE_CATALOGUES)
UNITS = ('Each','m','m\u00b2','m\u00b3','Hour')


def unified_shop(shop):
    return (shop.trade or 'mechanic') in TRADE_CATALOGUES


def construction_shop(shop):
    return shop.trade in CONSTRUCTION_TRADES or shop.trade == 'boq'


def categories_for(shop):
    return list(CONSTRUCTION_TRADES) if shop.trade == 'boq' else list(dict.fromkeys([shop.trade or 'mechanic', 'general']))


def default_category(shop):
    # Keep earlier BOQ custom rows and their codes in their original category.
    return shop.trade or 'mechanic'


def category_options(shop):
    keys = categories_for(shop)
    if shop.trade == 'boq':
        keys = ['boq'] + keys
    return [dict(key=key,label=CATEGORY_LABELS.get(key,'BOQ custom items')) for key in keys]


def work_items_for(trade):
    return [dict(code=code,description=description,unit=unit)
            for code,description,unit in BOQ_WORK_ITEMS.get(trade,())]


def custom_boq_code(trade):
    from uuid import uuid4
    return f'{BOQ_CODE_PREFIXES[trade]}-C-{uuid4().hex.upper()}'


def client_price_rows(user_id,shop,category,item_model,legacy_parts=()):
    """Read canonical rates plus eligible legacy rows without migrating source records."""
    from decimal import Decimal
    from types import SimpleNamespace
    if shop.user_id!=user_id:
        raise ValueError('Catalogue owner does not match client.')
    saved=item_model.query.filter_by(user_id=user_id,shop_id=shop.id,trade=category).order_by(item_model.id).all()
    codes={item.code for item in saved}
    names={item.description.strip().casefold() for item in saved}
    linked={item.legacy_part_id for item in saved if item.legacy_part_id is not None}
    if category!=(shop.trade or 'mechanic'):
        return saved
    for part in legacy_parts:
        code=part.item_code or f'{BOQ_CODE_PREFIXES[category]}-LEGACY-{part.id}'
        if code in codes or part.id in linked or part.part_name.strip().casefold() in names:
            continue
        # Legacy selection is scoped by the caller. Never infer a component split.
        rate=Decimal(str(part.default_price or 0))
        if rate.is_finite() and rate.as_tuple().exponent > -2:
            rate=rate.quantize(Decimal('0.01'))
        saved.append(SimpleNamespace(id='legacy:'+str(part.id),code=code,description=part.part_name,unit='Each',quantity=Decimal(1),
            material_rate=None,labour_rate=None,combined_rate=rate,total_rate=rate,total_amount=rate,
            legacy_part_id=part.id if part.shop_id==shop.id and part.user_id==user_id else None))
        codes.add(code)
    return saved


def quotation_catalogue(user_id, shop, item_model, shop_model, part_model=None, legacy_parts=()):
    """One client owns all category rates. Never discover prices in other shops."""
    from types import SimpleNamespace
    if shop.user_id != user_id:
        raise ValueError('Catalogue owner does not match client.')
    items, catalogues = [], category_options(shop)
    for category in catalogues:
        trade = category['key']
        saved = client_price_rows(user_id,shop,trade,item_model,legacy_parts)
        by_code = {item.code:item for item in saved}
        definitions = work_items_for(trade)
        definitions += [dict(code=item.code,description=item.description,unit=item.unit) for item in saved if item.code not in {entry['code'] for entry in definitions}]
        for definition in definitions:
            item = by_code.get(definition['code'])
            combined = item.combined_rate if item else None
            items.append(SimpleNamespace(id=item.id if item else ('template:'+definition['code'] if trade==shop.trade else f'template:{trade}:{definition["code"]}'),
                code=definition['code'],description=item.description if item else definition['description'],
                unit=item.unit if item else definition['unit'],material_rate=item.material_rate if item else None,
                labour_rate=item.labour_rate if item else None,combined_rate=combined,
                rate_basis='combined' if combined is not None else 'split',catalogue_key=trade,catalogue_trade=trade))
    return items,catalogues


def mechanic_quote_catalogue(user_id,shop,item_model,shop_model,part_model,legacy_parts):
    """Adapt the same rate choices to existing Mechanic part/labour controls."""
    from types import SimpleNamespace
    items,_=quotation_catalogue(user_id,shop,item_model,shop_model,part_model,legacy_parts)
    from collections import Counter
    names=Counter(item.description.casefold().strip() for item in items)
    return [SimpleNamespace(part_name=item.description if names[item.description.casefold().strip()]==1 else f'{CATEGORY_LABELS[item.catalogue_key]} / {item.code} / {item.description}',
        default_price=item.combined_rate if item.combined_rate is not None else item.material_rate,
        labour_rate=item.labour_rate,code=item.code,unit=item.unit) for item in items]


def decimal_value(value, quantity=False, combined=False):
    from decimal import Decimal, InvalidOperation
    try:
        number = Decimal(str(value))
        limit = Decimal('99999999999.999' if quantity else '999999999999.99')
        step = Decimal('0.001' if quantity else '0.01')
        if not number.is_finite() or number < 0 or (quantity and number == 0) or number > limit:
            raise ValueError('Enter a positive quantity and valid non-negative rates.')
        if not combined and number != number.quantize(step):
            raise ValueError('Use up to three decimal places for quantities and two for split rates.')
        return number
    except (InvalidOperation, TypeError):
        raise ValueError('Enter valid quantities and rates.') from None


def line_amount(quantity, material_rate=None, labour_rate=None, combined_rate=None):
    from decimal import Decimal, ROUND_HALF_UP, localcontext
    with localcontext() as ctx:
        ctx.prec = 60
        rate = Decimal(str(combined_rate)) if combined_rate is not None else Decimal(str(material_rate)) + Decimal(str(labour_rate))
        return (Decimal(str(quantity))*rate).quantize(Decimal('0.01'),rounding=ROUND_HALF_UP)


def snapshot_columns(line):
    """Render old snapshots unchanged and explicitly label new combined-only rates."""
    if line.get('combined_rate') is not None:
        return [line['code'],line['description'],line['unit'],line['quantity'],
                'Combined: '+str(line['combined_rate']),'—',line['total']]
    return [line[key] for key in ('code','description','unit','quantity','material_rate','labour_rate','total')]
