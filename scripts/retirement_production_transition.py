"""Explicit production adapter for tested RCM transitions. Read-only by default.

Uses only inherited RENDER_DATABASE_URL. Never loads the application factory.
All stages share one transaction, with identity/schema validation before commit.
"""
import contextlib
import io
import os
import sys
import sqlalchemy as sa
import psycopg2
import retirement_stage2_local as base
import retirement_stage1_owner_transition as founder
import retirement_waiting_room_transition as waiting
import retirement_relationship_transition as relationships


def identity(conn):
    result=tuple(conn.execute(sa.text('SELECT current_database(),session_user,current_user')).one())
    if result!=('ait_platform_db','aitplatformdb_retirement','ait_platform_db_user'):
        raise ValueError('Unexpected production identity')
    return result


def run(apply=False):
    engine=sa.create_engine('postgresql+psycopg2://',creator=lambda:psycopg2.connect(
        os.environ['RENDER_DATABASE_URL'],connect_timeout=20,
        options='-c search_path=public -c lock_timeout=5000 -c statement_timeout=30000'))
    with engine.begin() as conn:
        if not apply:conn.exec_driver_sql('SET TRANSACTION READ ONLY')
        identity(conn)
        revisions=conn.execute(sa.text('SELECT version_num FROM public.alembic_version ORDER BY version_num')).scalars().all()
        def unchanged_revision(c):
            assert c.execute(sa.text('SELECT version_num FROM public.alembic_version ORDER BY version_num')).scalars().all()==revisions
        base.identity=identity
        base.revision=unchanged_revision
        original_stage1=base.stage1
        uniques=sa.inspect(conn).get_unique_constraints('retirement_organisation',schema='public')
        original_stage1(conn,legacy_owner_unique=bool(uniques))
        present=sa.inspect(conn).has_table('retirement_waiting_user',schema='public')
        if present:
            waiting.verify(conn)
        else:
            # Preflight the unchanged legacy Stage 2 before the founder transition.
            base.stage1=lambda c:original_stage1(c,legacy_owner_unique=bool(uniques))
            module,metadata=base.definitions()
            base.verify(conn,metadata,module)
            base.stage1=original_stage1
        candidates=relationships.inventory(conn)
        counts={n:conn.execute(sa.text('SELECT count(*) FROM public.'+n)).scalar_one() for n in founder.TABLES}
        print('Production RCM preflight PASS:',counts,'evidenced legacy Staff:',len(candidates),flush=True)
        if not apply:return
        class SharedTransaction:
            @contextlib.contextmanager
            def begin(self):yield conn
            def dispose(self):pass
        base.local_engine=lambda:SharedTransaction()
        # The legacy helpers' console labels are local-specific; suppress them.
        # Their SQL, validation, dependencies and rollback checks remain intact.
        with contextlib.redirect_stdout(io.StringIO()):
            founder.transition()
            waiting.transition()
            relationships.transition()
        identity(conn);unchanged_revision(conn)
        result=waiting.verify(conn);result.update(relationships.verify(conn))
        print('Production RCM precommit validation PASS:',result,flush=True)
    with engine.connect() as conn:
        conn.exec_driver_sql('SET TRANSACTION READ ONLY')
        identity(conn);unchanged_revision(conn)
        result=waiting.verify(conn);result.update(relationships.verify(conn))
        print('Production RCM postcommit validation PASS:',result,flush=True)
    engine.dispose()


if __name__=='__main__':
    try:
        if sys.argv[1:] not in ([],['--apply']):raise ValueError('Use --apply to authorise writes')
        run(sys.argv[1:]==['--apply'])
    except Exception as error:
        print('RCM production operation stopped:',type(error).__name__)
        raise SystemExit(1)
