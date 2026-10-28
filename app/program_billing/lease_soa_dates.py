"""PM calendar dates: naive ledger dates are effective dates, not UTC audit times."""
from datetime import datetime
from zoneinfo import ZoneInfo

SOUTH_AFRICA=ZoneInfo('Africa/Johannesburg')


def today():
    return datetime.now(SOUTH_AFRICA).date()


def effective_date(value):
    if value is None:return None
    if isinstance(value,datetime):
        if value.tzinfo is not None:value=value.astimezone(SOUTH_AFRICA)
        return value.date()
    return value
