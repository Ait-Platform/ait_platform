# app/utils/db.py
import time
from sqlalchemy.exc import OperationalError

def commit_with_retry(session, retries=5, base_delay=0.2):
    for i in range(retries):
        try:
            session.commit()
            return
        except OperationalError as e:
            # SQLite: database is locked
            if "database is locked" in str(e).lower() and i < retries - 1:
                session.rollback()
                time.sleep(base_delay * (2 ** i))  # 0.2s, 0.4s, 0.8s, ...
                continue
            session.rollback()
            raise
