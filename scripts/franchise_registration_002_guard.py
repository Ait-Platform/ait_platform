"""Read-only by default; no app factory, role changes, automatic migration or credentials printed."""
import argparse, hashlib, ipaddress, os, socket, subprocess
from datetime import datetime, timezone
from pathlib import Path
import sqlalchemy as sa
from sqlalchemy.pool import NullPool
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.migration import HeadMaintainer

HOST='dpg-d4bkqsf5r7bs73989ia0-a'
TARGET='franchise_registration_002'
PARENT='ait_franchisee_001'
MIGRATION_HASH='d64248d84dba2cd5eb09a50327da7fa4a6433a0ddcb1b4010a08c3b668feb2d3'
ENV_HASH='4cb6af519e85872b9ac38576f18a8ca5ad06264435e45e54b9438e034625dea9'

class Stop(RuntimeError): pass

def require(value,message):
    if not value: raise Stop(message)

class PredictedHeads(HeadMaintainer):
    def _insert_version(self,rev): self.heads.add(rev)
    def _delete_version(self,rev): self.heads.remove(rev)
    def _update_version(self,old,new): self.heads.remove(old);self.heads.add(new)

def identity(c, url):
    driver = c.connection.driver_connection.get_dsn_parameters()
    require(driver.get("host") == HOST, "Actual driver hostname mismatch")
    require(driver.get("dbname") == "ait_platform_db", "Driver database mismatch")
    require(str(driver.get("port")) == "5432", "Driver port mismatch")
    row = dict(c.execute(sa.text("""SELECT current_database() AS database,
      session_user AS login, current_user AS effective_role,
      current_setting('role') AS active_role, current_schema() AS schema,
      inet_server_addr()::text AS backend_address, inet_server_port() AS port,
      current_setting('server_version') AS version,
      current_setting('server_version_num')::int AS version_number,
      pg_is_in_recovery() AS replica""")).mappings().one())
    expected = dict(database="ait_platform_db", login="aitplatformdb_retirement",
        effective_role="ait_platform_db_user", active_role="ait_platform_db_user",
        schema="public", port=5432)
    for key, value in expected.items(): require(row[key] == value, key + " mismatch")
    require(not row["replica"], "Connected to a recovery/replica instance")
    require(row["version_number"] // 10000 == 18, "Review PostgreSQL major-version compatibility")
    backend = str(ipaddress.ip_interface(row["backend_address"]).ip)
    try: dns = sorted({item[4][0] for item in socket.getaddrinfo(HOST, 5432)})
    except OSError: dns = ["DNS diagnostic unavailable"]
    print("Verified identity:", expected, "PostgreSQL:", row["version"])
    print("Informational only - DNS:", dns, "backend:", backend)
    for table in ("alembic_version", "ait_franchisee_registration"):
        visible = c.scalar(sa.text("SELECT to_regclass(:name)=to_regclass(:qualified)"),
            dict(name=table, qualified="public."+table))
        require(visible is True, "Unexpected schema resolution: " + table)
    require(c.scalar(sa.text("SELECT has_schema_privilege(current_user,'public','USAGE')")), "Missing schema USAGE")
    require(c.scalar(sa.text("""SELECT r.rolsuper OR pg_has_role(current_user,t.relowner,'USAGE')
      FROM pg_class t JOIN pg_roles r ON r.rolname=current_user
      WHERE t.oid='public.ait_franchisee_registration'::regclass""")), "Effective role cannot ALTER the registration table")
    for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
        require(c.scalar(sa.text("SELECT has_table_privilege(current_user,'public.alembic_version',:p)"), dict(p=privilege)), "Missing Alembic permission: " + privilege)
    for privilege in ("SELECT", "INSERT", "UPDATE"):
        require(c.scalar(sa.text("SELECT has_table_privilege(current_user,'public.ait_franchisee_registration',:p)"), dict(p=privilege)), "Missing registration permission: " + privilege)
    sequence = c.scalar(sa.text("SELECT pg_get_serial_sequence('public.ait_franchisee_registration','id')"))
    require(sequence and c.scalar(sa.text("SELECT has_sequence_privilege(current_user,:s,'USAGE')"), dict(s=sequence)), "Missing registration sequence USAGE")
    require(c.scalar(sa.text("SELECT NOT relrowsecurity FROM pg_class WHERE oid='public.ait_franchisee_registration'::regclass")), 'RLS would prevent an unrestricted registration snapshot')
    print("Effective role DDL, schema, Alembic, registration and sequence permissions verified.")

def snapshot(c,oid):
    rows=c.execute(sa.text("SELECT count(*),md5(coalesce(string_agg(md5(to_jsonb(r)::text),'' ORDER BY id),'')) FROM public.ait_franchisee_registration r")).one()
    return metadata(c,oid),tuple(rows)


def metadata(c,oid):
    result=[]
    queries=[
      "SELECT relowner,relacl::text,relrowsecurity,relpersistence FROM pg_class WHERE oid=:oid",
      "SELECT attnum,attname,atttypid,atttypmod,attnotnull,attidentity,attgenerated FROM pg_attribute WHERE attrelid=:oid AND attnum>0 AND NOT attisdropped ORDER BY attnum",
      "SELECT adnum,pg_get_expr(adbin,adrelid) FROM pg_attrdef WHERE adrelid=:oid ORDER BY adnum",
      "SELECT to_jsonb(k)::text FROM pg_constraint k WHERE conrelid=:oid OR confrelid=:oid ORDER BY oid",
      "SELECT to_jsonb(i)::text FROM pg_index i WHERE indrelid=:oid ORDER BY indexrelid",
      "SELECT to_jsonb(t)::text FROM pg_trigger t WHERE tgrelid=:oid ORDER BY oid",
      "SELECT last_value,is_called FROM public.ait_franchisee_number_seq",
    ]
    for sql in queries: result.append([tuple(r) for r in c.execute(sa.text(sql),{'oid':oid})])
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--expected-commit',required=True)
    p.add_argument('--apply',action='store_true')
    p.add_argument('--authorisation',default='')
    p.add_argument('--backup-reference',default='')
    p.add_argument('--backup-completed-utc',default='')
    p.add_argument('--backup-recoverable-confirmed',action='store_true')
    args=p.parse_args()
    require(subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==args.expected_commit,'Deployed commit mismatch')
    require(subprocess.run(['git','diff','--quiet','HEAD','--','migrations'],check=False).returncode==0,'Migration sources have local changes')
    for path,wanted in [('migrations/versions/franchise_registration_002.py',MIGRATION_HASH),('migrations/env.py',ENV_HASH)]:
        require(hashlib.sha256(Path(path).read_bytes().replace(b'\r\n',b'\n')).hexdigest()==wanted,'Source hash mismatch: '+path)
    if args.apply:
        require(args.authorisation=='AUTHORISE ONLY franchise_registration_002','Explicit migration authorisation is required')
        require(args.backup_reference.strip() and args.backup_recoverable_confirmed,'A current recoverable backup must be confirmed')
        backup=datetime.fromisoformat(args.backup_completed_utc.replace('Z','+00:00'))
        require(backup.tzinfo is not None and 0 <= (datetime.now(timezone.utc)-backup).total_seconds() <= 86400,'Backup must be within 24 hours')
    raw=os.environ.get('DATABASE_URL')
    require(raw,'Render-assigned DATABASE_URL is missing')
    if raw.startswith('postgres://'):raw='postgresql://'+raw[len('postgres://'):]
    url=sa.engine.make_url(raw)
    require(url.get_backend_name()=='postgresql' and url.host==HOST and url.database=='ait_platform_db' and (url.port or 5432)==5432,'Configured production identity mismatch')
    cfg=Config('alembic.ini');cfg.attributes['skip_app_metadata']=True
    scripts=ScriptDirectory.from_config(cfg)
    revision=scripts.get_revision(TARGET)
    require(revision.down_revision==PARENT and revision.dependencies is None,'Unexpected rename ancestry')
    engine=sa.create_engine(url,poolclass=NullPool,hide_parameters=True,connect_args={'connect_timeout':10})
    with engine.begin() as c:
        if not args.apply:c.exec_driver_sql('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        c.exec_driver_sql("SET LOCAL lock_timeout='5s'")
        c.exec_driver_sql("SET LOCAL statement_timeout='30s'")
        identity(c,url)
        for permission in ('USAGE','SELECT'):
            require(c.scalar(sa.text("SELECT has_sequence_privilege(current_user,'public.ait_franchisee_number_seq',:p)"),{'p':permission}),'Missing registration-number sequence '+permission)
        require(c.scalar(sa.text("SELECT to_regclass('public.franchise_registration') IS NULL")),'New table already exists; review recorded migration/schema state')
        if args.apply:
            c.exec_driver_sql('LOCK TABLE public.alembic_version IN EXCLUSIVE MODE')
            c.exec_driver_sql('LOCK TABLE public.ait_franchisee_registration IN ACCESS EXCLUSIVE MODE')
        heads=tuple(c.scalars(sa.text('SELECT version_num FROM public.alembic_version ORDER BY version_num')))
        ancestors=set()
        for head in heads:ancestors.update(r.revision for r in scripts.walk_revisions(base='base',head=head))
        require(PARENT in ancestors and TARGET not in ancestors,'Unexpected applied rename ancestry')
        plan=scripts._upgrade_revs(TARGET,heads)
        require([step.revision.revision for step in plan]==[TARGET],'Refusing prerequisites or unrelated migrations')
        predicted=PredictedHeads(None,heads)
        for step in plan:predicted.update_to_step(step)
        expected=(set(heads)-{PARENT})|{TARGET}
        require(predicted.heads==expected and len(expected)==len(heads),'Unrelated migration heads would change')
        oid=c.scalar(sa.text("SELECT 'public.ait_franchisee_registration'::regclass::oid"))
        require(c.scalar(sa.text("SELECT count(*) FROM public.ait_franchisee_registration WHERE user_id=644 AND franchisee_number='FR-0001'"))==1,'Expected Bill FR-0001 baseline is missing; stop for review')
        before=snapshot(c,oid)
        print('Verified preflight. Heads:',heads,'registration count/digest:',before[1])
        print('Exact plan:',[TARGET],'expected final heads:',sorted(expected))
        if args.apply:
            cfg.attributes.update(connection=c,franchise_registration_rename_approved=True)
            command.upgrade(cfg,TARGET)
            require(c.scalar(sa.text("SELECT to_regclass('public.ait_franchisee_registration') IS NULL")),'Legacy name remains unexpectedly')
            require(c.scalar(sa.text("SELECT 'public.franchise_registration'::regclass::oid"))==oid,'Original table OID changed')
            after_rows=tuple(c.execute(sa.text("SELECT count(*),md5(coalesce(string_agg(md5(to_jsonb(r)::text),'' ORDER BY id),'')) FROM public.franchise_registration r")).one())
            require(metadata(c,oid)==before[0] and after_rows==before[1],'Rows, dates, ciphertext, constraints, indexes, grants, relationships or sequence changed')
            after=set(c.scalars(sa.text('SELECT version_num FROM public.alembic_version')))
            require(after==expected,'Final heads do not match; rolling back')
    print('COMMITTED only franchise_registration_002; all preservation checks passed.' if args.apply else 'READ-ONLY PREFLIGHT PASSED. No migration executed; explicit authorisation and a current backup are still required.')

if __name__=='__main__':
    try:main()
    except Stop as error:print('STOP:',error);raise SystemExit(1)
    except Exception as error:
        print('STOP:',type(error).__name__,'- operation failed; any active transaction rolled back. No credentials or private records printed.')
        raise SystemExit(1)
