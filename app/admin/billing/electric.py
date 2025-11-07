# UPSERT a single electricity meter/month row into bil_metsoa_meter_month
from datetime import datetime
from sqlalchemy import text
from app import db

def upsert_electricity_line(
    *, tenant_id: int, meter_id: int, month: str,
    prev_date: str | None, prev_read: int | None,
    curr_date: str | None, curr_read: int | None,
    days: int | None, consumption: float | int | None,
    rate: float, due: float,
) -> None:
    month = datetime.strptime(month, "%Y-%m").strftime("%Y-%m")
    db.session.execute(text("""
        INSERT INTO bil_metsoa_meter_month
            (tenant_id, meter_id, month, utility_type,
             prev_date, prev_read, curr_date, curr_read, days, consumption,
             elec_rate, elec_due, total_due, updated_at)
        VALUES (:t, :m, :mon, 'electricity',
                :pd, :pr, :cd, :cr, :dy, :cons,
                :rate, :due, :due, CURRENT_TIMESTAMP)
        ON CONFLICT (tenant_id, meter_id, month)
        DO UPDATE SET
            utility_type = 'electricity',
            prev_date    = excluded.prev_date,
            prev_read    = excluded.prev_read,
            curr_date    = excluded.curr_date,
            curr_read    = excluded.curr_read,
            days         = excluded.days,
            consumption  = excluded.consumption,
            elec_rate    = excluded.elec_rate,
            elec_due     = excluded.elec_due,
            total_due    = excluded.total_due,
            updated_at   = CURRENT_TIMESTAMP
    """), {
        "t": tenant_id, "m": meter_id, "mon": month,
        "pd": prev_date, "pr": prev_read, "cd": curr_date, "cr": curr_read,
        "dy": days, "cons": consumption,
        "rate": round(rate or 0.0, 2), "due": round(due or 0.0, 2),
    })
    db.session.commit()
