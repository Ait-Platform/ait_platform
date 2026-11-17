"""Local-only, read-only fresh-process startup/import probe; never renders PDFs."""
import argparse
import contextlib
import ctypes
import io
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def memory():
    if sys.platform == 'win32':
        size = ctypes.c_size_t
        class Counters(ctypes.Structure):
            _fields_ = [('cb', ctypes.c_uint32), ('faults', ctypes.c_uint32)] + [
                (key, size) for key in ('peak_ws','ws','peak_paged','paged','peak_nonpaged',
                                       'nonpaged','pagefile','peak_pagefile','private')]
        counter = Counters();counter.cb = ctypes.sizeof(counter)
        kernel = ctypes.WinDLL('kernel32');kernel.GetCurrentProcess.restype = ctypes.c_void_p
        psapi = ctypes.WinDLL('psapi')
        psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32]
        if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(counter), counter.cb):
            raise ctypes.WinError()
        return dict(working_set_bytes=counter.ws, private_commit_bytes=counter.private,
                    peak_working_set_bytes=counter.peak_ws)
    values = {}
    path = Path('/proc/self/smaps_rollup')
    if path.exists():
        for line in path.read_text().splitlines():
            fields = line.split()
            if len(fields) >= 3 and fields[1].isdigit():values[fields[0].rstrip(':')] = int(fields[1])*1024
    return dict(rss_bytes=values.get('Rss'), pss_bytes=values.get('Pss'),
                private_resident_bytes=values.get('Private_Clean',0)+values.get('Private_Dirty',0))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('imports','factory','alembic'))
    parser.add_argument('--lightweight', action='store_true', help='Skip app metadata for operational Alembic commands')
    args=parser.parse_args()
    started=time.perf_counter();baseline=memory()
    from dotenv import dotenv_values
    from sqlalchemy import event
    from sqlalchemy.engine import Engine, make_url
    raw=dotenv_values(ROOT/'.env').get('DATABASE_URL','')
    url=make_url(raw.replace('postgres://','postgresql://',1))
    if url.database!='ait_local_db' or url.host not in ('localhost','127.0.0.1','::1') or url.query:
        raise RuntimeError('This probe requires the verified localhost ait_local_db without overrides')
    os.environ['DATABASE_URL']=raw
    blocked=[]
    def guard(conn,cursor,statement,parameters,context,many):
        if statement.lstrip().split()[0].upper() not in ('SELECT','SHOW','SET'):
            blocked.append(statement.lstrip().split()[0].upper())
            raise RuntimeError('Startup mutation blocked by local memory probe')
    event.listen(Engine,'before_cursor_execute',guard)
    endpoints={}
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        if args.mode=='imports':
            import app
        elif args.mode=='factory':
            from app import create_app
            application=create_app({'TESTING':True,'SQLALCHEMY_DATABASE_URI':url,
                'SQLALCHEMY_ENGINE_OPTIONS':{'connect_args':{'options':'-c default_transaction_read_only=on'}}},operator_mode=True)
            endpoints={name:name in application.view_functions for name in (
                'admin_bp.index','admin_bp.adv_math_ai_scan_paper','franchisee_bp.register_index','mechanic_bp.protrade_output')}
            from app.extensions import db
            with application.app_context():db.session.remove();db.engine.dispose()
        else:
            from alembic.config import Config
            from alembic import command
            config=Config(str(ROOT/'migrations/alembic.ini'))
            config.attributes['skip_app_metadata']=args.lightweight
            command.current(config)
    families=('matplotlib','numpy','pandas','google.genai','google.generativeai','weasyprint',
              'playwright','reportlab','xhtml2pdf','av','azure','bpy')
    if blocked:raise RuntimeError('Startup attempted SQL mutations; measurement is not a clean read-only startup')
    print(json.dumps(dict(mode=args.mode,lightweight=args.lightweight,platform=sys.platform,seconds=round(time.perf_counter()-started,3),
        baseline=baseline,final=memory(),module_count=len(sys.modules),
        application_imported='app' in sys.modules,blocked_sql_count=len(blocked),
        endpoints=endpoints,
        heavy_imports={name:any(key==name or key.startswith(name+'.') for key in sys.modules) for name in families}),sort_keys=True))


if __name__=='__main__':main()
