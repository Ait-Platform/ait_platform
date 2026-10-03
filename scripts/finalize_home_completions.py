"""Explicit HOME housekeeping, without application factory/startup maintenance.

Run from a verified checkout with authoritative DATABASE_URL. Requires explicit
expected database and role; never calls the historical cutover service.
"""
import argparse
import os
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-database', required=True)
    parser.add_argument('--expected-role', required=True)
    parser.add_argument('--execute', action='store_true', required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from flask import Flask
    from sqlalchemy import text
    from sqlalchemy.engine import make_url
    from app.extensions import db
    from app.program_sace_home.lifecycle import finalize_due_completions
    url = os.environ['DATABASE_URL']
    if url.startswith('postgres://'):
        url = 'postgresql://' + url[len('postgres://'):]
    if make_url(url).get_backend_name() != 'postgresql':
        raise SystemExit('PostgreSQL is required.')
    app = Flask('home-completion-housekeeping')
    app.config.update(SQLALCHEMY_DATABASE_URI=url, SQLALCHEMY_TRACK_MODIFICATIONS=False,
        SQLALCHEMY_ENGINE_OPTIONS={'connect_args': {'connect_timeout': 15, 'options': '-c lock_timeout=10000'}})
    db.init_app(app)
    with app.app_context():
        try:
            identity = db.session.execute(text('SELECT current_database(), current_user')).one()
            if tuple(identity) != (args.expected_database, args.expected_role):
                raise ValueError('Database identity mismatch.')
            ids = finalize_due_completions()
            db.session.commit()
            print('Finalized HOME engagements:', ids)
        except Exception as exc:
            db.session.rollback()
            print('Housekeeping failed; verify state before retrying. Error type:', type(exc).__name__)
            raise SystemExit(1)


if __name__ == '__main__':
    main()
