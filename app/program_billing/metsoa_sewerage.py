"""Domestic discharge quantities, independent of dated tariff prices.

Verification is per meter/account; tariff availability alone does not establish
consumer eligibility. No category, exemption or rate is inferred from its name.
"""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

WIDTHS = (Decimal('.2'), Decimal('.633'), Decimal('.167'), Decimal('.5'))
DISCHARGE = tuple(map(Decimal, ('.95', '.75', '.75', '.65', '.60')))
CODES = ('S01', 'S02', 'S03', 'S04', 'S05')


def readiness(tiers, verification):
    v = verification or {}
    if not (v.get('verified') is True
            and v.get('category') == 'domestic_not_exceeding_two_storeys'
            and v.get('tank_arrangement') in ('individual_break_pressure_tank', 'no_individual_break_pressure_tank')
            and v.get('first_band_exempt') in (True, False)
            and isinstance(v.get('first_band_exempt'), bool)
            and v.get('tariff_category_confirmed') is True
            and v.get('source_reference')):
        return ['Sewerage category, tank arrangement, exemption status and matching tariff category require verification; sewerage charges are unavailable.']
    if v['tank_arrangement'] == 'individual_break_pressure_tank' and not v['first_band_exempt']:
        return ['Sewerage first-band exemption conflicts with the verified tank category; review the category evidence.']
    by_code = {str(t.get('code', '')).upper(): t for t in tiers}
    if len(by_code) != len(tiers) or set(by_code) != set(CODES):
        return ['Sewerage requires five unambiguous dated rates S01-S05; no fifth-band rate was assumed.']
    for code, tier in by_code.items():
        try:
            rate = Decimal(str(tier['rate']))
            if not rate.is_finite() or rate < 0:
                raise ValueError()
        except (KeyError, ValueError, InvalidOperation):
            return ['Sewerage rate unavailable for ' + code + '.']
    return []


def calculate(consumption, days, tiers, surcharge_rate, verification):
    volume = Decimal(str(consumption))
    if not volume.is_finite() or volume < 0 or days <= 0:
        raise ValueError('Sewerage requires nonnegative consumption and a positive reading interval.')
    if readiness(tiers, verification):
        return [], Decimal('0.00')
    rates = {t['code'].upper(): t for t in tiers}
    remaining = volume
    rows = []
    chargeable = Decimal(0)
    money = lambda amount: amount.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
    for index, code in enumerate(CODES):
        # The fifth band receives the finite measured remainder, never an
        # invented daily width or the previous band's unlimited multiplier.
        water = min(remaining, WIDTHS[index] * Decimal(str(days))) if index < 4 else remaining
        remaining -= water
        discharged = water * DISCHARGE[index]
        exempt = index == 0 and verification['first_band_exempt']
        priced_volume = Decimal(0) if exempt else discharged
        chargeable += priced_volume
        tier = rates[code]
        rate = Decimal(str(tier['rate']))
        rows.append(dict(desc=tier.get('label', code), water_cons=water,
                         discharge_fraction=DISCHARGE[index], cons=discharged,
                         chargeable_cons=priced_volume, rate=rate,
                         due=money(priced_volume * rate), exempt=exempt))
    if surcharge_rate is not None:
        rate = Decimal(str(surcharge_rate))
        if not rate.is_finite() or rate < 0:
            raise ValueError('Invalid sewerage infrastructure surcharge rate.')
        rows.append(dict(desc='Sewerage surcharge', cons=chargeable,
                         rate=rate, due=money(chargeable * rate)))
    return rows, sum((r['due'] for r in rows), Decimal('0.00'))
