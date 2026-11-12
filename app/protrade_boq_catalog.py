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
BOQ_CODE_PREFIXES = {'carpentry': 'CAR'}


def work_items_for(trade):
    return [dict(code=code, description=description, unit=unit)
            for code, description, unit in BOQ_WORK_ITEMS.get(trade, ())]


def custom_boq_code(trade):
    from uuid import uuid4
    return f'{BOQ_CODE_PREFIXES[trade]}-C-{uuid4().hex.upper()}'
