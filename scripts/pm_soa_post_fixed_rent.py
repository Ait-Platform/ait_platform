"""PM-only daily fixed-charge worker. Run from the service repo; never calls create_app()."""
import argparse
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def main():
    parser=argparse.ArgumentParser(description='Post due PM Fixed Charges only.')
    parser.add_argument('--run',action='store_true',help='Execute due fixed-charge posting using the assigned DATABASE_URL.')
    args=parser.parse_args()
    if not args.run:
        print('No transactions posted. Use --run only in the authorised PM scheduled job.')
        return 0
    raw=os.environ.get('DATABASE_URL','')
    if not raw:raise SystemExit('Assigned DATABASE_URL is required.')
    if raw.startswith('postgres://'):raw='postgresql://'+raw[len('postgres://'):]
    from sqlalchemy.engine import make_url
    if make_url(raw).get_backend_name()!='postgresql':raise SystemExit('PostgreSQL is required.')
    from flask import Flask
    from app.extensions import db
    app=Flask('pm-soa-fixed-rent-worker')
    app.config.update(SQLALCHEMY_DATABASE_URI=raw,SQLALCHEMY_TRACK_MODIFICATIONS=False,
        SQLALCHEMY_ENGINE_OPTIONS={'pool_pre_ping':True})
    db.init_app(app)
    from app.program_billing.lease_soa_fixed import run_due
    with app.app_context():
        try:result=run_due()
        except Exception:
            db.session.rollback()
            print('PM fixed-charge worker failed. Check migration, ownership and assigned database configuration.')
            return 1
        print(json.dumps(result))
        return 1 if result['failed'] else 0


if __name__=='__main__':
    raise SystemExit(main())
