"""Explicit local-only Retirement provisioning/verification. Never imports create_app."""
import ast
import importlib.util
import ipaddress
import json
from pathlib import Path
import sys
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable, CreateIndex
from sqlalchemy.engine import make_url
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]

def migration_source():
    path = ROOT / "migrations/versions/retire_02_membership.py"
    spec = importlib.util.spec_from_file_location("retirement_stage2_source", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, ast.parse(path.read_text(encoding="utf-8"))


def definitions():
    module, tree = migration_source()
    metadata = sa.MetaData()
    sa.Table("user", metadata, sa.Column("id", sa.Integer, primary_key=True))
    sa.Table("retirement_organisation", metadata, sa.Column("id", sa.Integer, primary_key=True),
             sa.Column("owner_user_id", sa.Integer))
    # Read only schema expressions; NEVER call migration upgrade/downgrade.
    upgrade = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "upgrade")
    for node in upgrade.body:
        call = node.value if isinstance(node, (ast.Assign, ast.Expr)) else None
        if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute):
            continue
        if not isinstance(call.func.value, ast.Name) or call.func.value.id != "op":
            continue
        if call.func.attr not in ("create_table", "create_index"):
            continue
        args = [eval(compile(ast.Expression(arg), "<reviewed-schema>", "eval"), {"sa": sa}) for arg in call.args]
        if call.func.attr == "create_table":
            sa.Table(args[0], metadata, *args[1:])
        else:
            sa.Index(args[0], *(metadata.tables[args[1]].c[name] for name in args[2]))
    return module, metadata


def local_engine():
    original = make_url(dotenv_values(ROOT / ".env")["DATABASE_URL"])
    assert original.drivername == "postgresql+psycopg2"
    assert original.database == "ait_local_db" and original.host in ("localhost", "127.0.0.1")
    assert original.port == 5432 and not original.query
    url = original.set(host="127.0.0.1")
    return sa.create_engine(url, connect_args={"connect_timeout": 5,
        "options": "-c search_path=public -c statement_timeout=15000 -c lock_timeout=5000"})


def identity(conn):
    row = conn.execute(sa.text("SELECT current_database() AS db, host(inet_server_addr()) AS host, inet_server_port() AS port, current_schema() AS schema")).mappings().one()
    assert row["db"] == "ait_local_db" and ipaddress.ip_address(row["host"]).is_loopback
    assert row["port"] == 5432 and row["schema"] == "public"
    return dict(row)


def revision(conn):
    assert conn.execute(sa.text("SELECT version_num FROM public.alembic_version")).scalars().all() == ["add_letterhead_to_sender"]


def sequence_check(conn, table):
    result = conn.execute(sa.text("""SELECT s.seqtypid::regtype::text AS type,s.seqincrement,s.seqcycle,a.attname,d.deptype,
       pg_get_expr(ad.adbin,ad.adrelid) AS default_expr,
       EXISTS(SELECT 1 FROM pg_depend x WHERE x.classid='pg_attrdef'::regclass AND x.objid=ad.oid AND x.refclassid='pg_class'::regclass AND x.refobjid=s.seqrelid) AS default_uses_sequence
       FROM pg_sequence s JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=s.seqrelid
       JOIN pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid
       JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
       WHERE s.seqrelid=pg_get_serial_sequence(:table,'id')::regclass
       AND d.refobjid=to_regclass(:table) AND d.deptype IN ('a','i')"""), {"table": "public."+table}).mappings().one()
    assert result["type"] == "integer" and result["seqincrement"] == 1 and not result["seqcycle"]
    assert result["attname"] == "id" and result["default_uses_sequence"]
    assert "nextval" in result["default_expr"]  # dependency OIDs, not textual sequence spelling, establish linkage


def stage1(conn, legacy_owner_unique=False):
    ins=sa.inspect(conn)
    assert ins.has_table("retirement_organisation",schema="public")
    cols=ins.get_columns("retirement_organisation",schema="public")
    assert [(c["name"],str(c["type"]),c["nullable"]) for c in cols] == [("id","INTEGER",False),("name","VARCHAR(200)",False),("owner_user_id","INTEGER",False)]
    assert ins.get_pk_constraint("retirement_organisation",schema="public")["constrained_columns"] == ["id"]
    assert [u["column_names"] for u in ins.get_unique_constraints("retirement_organisation",schema="public")] == ([["owner_user_id"]] if legacy_owner_unique else [])
    fks=ins.get_foreign_keys("retirement_organisation",schema="public")
    assert len(fks)==1 and fks[0]["constrained_columns"]==["owner_user_id"] and fks[0]["referred_table"]=="user" and fks[0]["referred_columns"]==["id"] and fks[0]["referred_schema"] in (None,"public")
    sequence_check(conn,"retirement_organisation")
    uid=next(c for c in ins.get_columns("user",schema="public") if c["name"]=="id")
    assert str(uid["type"])=="INTEGER" and not uid["nullable"]
    assert ins.get_pk_constraint("user",schema="public")["constrained_columns"]==["id"]
    assert conn.execute(sa.text('SELECT count(*) FROM public.retirement_organisation o WHERE NOT EXISTS (SELECT 1 FROM public."user" u WHERE u.id=o.owner_user_id)')).scalar_one()==0


def verify(conn, metadata, module):
    identity(conn);revision(conn);stage1(conn)
    ins=sa.inspect(conn)
    for name in ("retirement_role","retirement_membership"):
        table=metadata.tables[name]
        columns=ins.get_columns(name,schema="public")
        assert [(c["name"],c["type"].compile(dialect=conn.dialect),c["nullable"]) for c in columns] == [(c.name,c.type.compile(dialect=conn.dialect),c.nullable) for c in table.columns], name
        for column in columns:
            if column["name"] == "requested_at":
                # Compare transaction-time semantics; tolerate PostgreSQL equivalent spelling.
                expression=column["default"]
                assert expression and expression.lower().strip() in ("now()","current_timestamp","transaction_timestamp()")
                assert conn.execute(sa.text("SELECT ("+expression+") = CURRENT_TIMESTAMP")).scalar_one()
            elif column["name"] != "id":
                assert column["default"] is None
        assert ins.get_pk_constraint(name,schema="public")["constrained_columns"]==["id"]
        unique=sorted(tuple(u["column_names"]) for u in ins.get_unique_constraints(name,schema="public"))
        expected=sorted(tuple(c.name for c in u.columns) for u in table.constraints if isinstance(u,sa.UniqueConstraint))
        assert unique==expected
        actual_fk=sorted((tuple(f["constrained_columns"]),f["referred_table"],tuple(f["referred_columns"])) for f in ins.get_foreign_keys(name,schema="public"))
        expected_fk=sorted(((f.parent.name,),f.column.table.name,(f.column.name,)) for f in table.foreign_keys)
        assert actual_fk==expected_fk
        assert all(f["referred_schema"] in (None,"public") and not f["options"] for f in ins.get_foreign_keys(name,schema="public"))
        actual_ix=sorted((i["name"],tuple(i["column_names"]),i["unique"]) for i in ins.get_indexes(name,schema="public") if not i.get("duplicates_constraint"))
        assert actual_ix==sorted((i.name,tuple(c.name for c in i.columns),i.unique) for i in table.indexes)
        checks=ins.get_check_constraints(name,schema="public")
        assert {c["name"] for c in checks}=={c.name for c in table.constraints if isinstance(c,sa.CheckConstraint)}
        # Truth-table comparison handles PostgreSQL's ANY/array/cast rewrites semantically.
        for check in checks:
            expected_check=next(c for c in table.constraints if isinstance(c,sa.CheckConstraint) and c.name==check["name"])
            for status in ("pending","active","denied","disabled","invalid",""):
                for role in (None,1):
                    sql="SELECT ("+check["sqltext"]+") IS NOT DISTINCT FROM ("+str(expected_check.sqltext)+") FROM (SELECT CAST(:status AS varchar(16)) AS status, CAST(:role AS integer) AS approved_role_id) s"
                    assert conn.execute(sa.text(sql),{"status":status,"role":role}).scalar_one()
        sequence_check(conn,name)
    assert set(conn.execute(sa.text("SELECT code,name FROM public.retirement_role")).all())==set(module.ROLES)
    owner_id=conn.execute(sa.text("SELECT id FROM public.retirement_role WHERE code='organisation_owner'")).scalar_one()
    missing=conn.execute(sa.text("""SELECT count(*) FROM public.retirement_organisation o LEFT JOIN public.retirement_membership m ON m.organisation_id=o.id AND m.user_id=o.owner_user_id WHERE m.id IS NULL OR m.status<>'active' OR m.approved_role_id IS DISTINCT FROM :role OR m.requested_role_id IS NOT NULL"""),{"role":owner_id}).scalar_one()
    assert missing==0
    return {name:conn.execute(sa.text('SELECT count(*) FROM public.'+name)).scalar_one() for name in ('retirement_organisation','retirement_membership','retirement_role')}


def provision():
    module,metadata=definitions();engine=local_engine()
    with engine.begin() as conn:
        print("Verified write target:",identity(conn));revision(conn);stage1(conn)
        names=['retirement_role','retirement_role_id_seq','retirement_membership','retirement_membership_id_seq','retirement_role_pkey','retirement_role_code_key','retirement_membership_pkey','uq_retirement_membership_org_user','ix_retirement_membership_org_status','ix_retirement_membership_user_id']
        assert conn.execute(sa.text("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND (c.relname=ANY(:names) OR c.relname LIKE 'retirement_role%' OR c.relname LIKE 'retirement_membership%')"),{'names':names}).scalar_one()==0
        assert conn.execute(sa.text("SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public' AND t.typname IN ('retirement_role','retirement_membership','_retirement_role','_retirement_membership')")).scalar_one()==0
        check_names=[c.name for c in metadata.tables['retirement_membership'].constraints if c.name]
        assert conn.execute(sa.text("SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='public' AND c.conname=ANY(:names)"),{'names':check_names}).scalar_one()==0
        for name in ('retirement_role','retirement_membership'):
            conn.execute(CreateTable(metadata.tables[name]))
            for index in metadata.tables[name].indexes:
                conn.execute(CreateIndex(index))
        ids=module.establish_roles(conn,metadata.tables['retirement_role'])
        module.backfill_owner_memberships(conn,metadata.tables['retirement_organisation'],metadata.tables['retirement_membership'],ids['organisation_owner'])
        counts=verify(conn,metadata,module)
        assert counts['retirement_membership']==counts['retirement_organisation']
        print('Precommit schema/sequence/constraint/index/role/coverage checks passed:',counts)
    print('Provisioning COMMITTED in one transaction. Alembic unchanged.')
    engine.dispose()

if __name__=='__main__':
    if sys.argv[1:] == ['provision']:
        provision()
    elif sys.argv[1:] == ['verify']:
        module,metadata=definitions();engine=local_engine()
        with engine.connect() as conn:
            conn.exec_driver_sql('SET TRANSACTION READ ONLY')
            if sa.inspect(conn).has_table('retirement_waiting_user',schema='public'):
                from retirement_waiting_room_transition import verify as verify_waiting
                print('Verified:',verify_waiting(conn))
            else:
                print('Verified:',verify(conn,metadata,module))
        engine.dispose()
    else:
        raise SystemExit('Specify provision or verify explicitly')
