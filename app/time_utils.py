# utils/time_utils.py
from datetime import datetime, timedelta, timezone
from datetime import datetime


SA_TIMEZONE = timezone(timedelta(hours=2))  # South Africa Standard Time

def app_now():
    """Canonical app clock: system time in SA timezone."""
    return datetime.now(SA_TIMEZONE)

def expiry_for(subject, mode="trial"):
    days = float(subject.trial_days) if mode == "trial" else float(subject.paid_days)
    minutes = days * 24 * 60  # convert days to minutes
    return app_now() + timedelta(minutes=minutes)

def minutes_until_expiry(expires_at):
    """Return whole minutes until expiry from local app clock."""
    delta = expires_at - app_now()
    return int(delta.total_seconds() // 60)