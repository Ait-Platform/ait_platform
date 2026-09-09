"""Phase 10 acceptance/security tests in the disposable local UIP schema."""
import io
import re
from datetime import date
from decimal import Decimal
from pathlib import Path
import pytest
import sqlalchemy as sa
from werkzeug.exceptions import HTTPException
from werkzeug.datastructures import FileStorage
from bootstrap import db, uip, ROOT
from app.uip.services import finance as f, documents
from phase3_helpers import order, act, key
from conftest import migrate_phase10

BASE = "/uip/manor-gardens/finance"


def values(**changes):
    return dict(transaction_date=date.today().isoformat(), amount="100.00", kind="INCOME",
        category="Repairs", description="Internal approved purpose", public_description="Streetlight repairs",
        public_party="Precinct Electrical Services", visibility="MEMBERS", request_key=key(), **changes) if not changes else {
        **dict(transaction_date=date.today().isoformat(), amount="100.00", kind="INCOME", category="Repairs",
        description="Internal approved purpose", public_description="Streetlight repairs", public_party="Precinct Electrical Services",
        visibility="MEMBERS", request_key=key()), **changes}


def post(client, path, payload):
    result = client.safe_post(path, payload)
    assert result.status_code == 302, (result.status_code, result.get_data(as_text=True)[:300])
    return result


def tx(data, **changes):
    return f.create_transaction(data.org.id, data.users["manager"].id, values(**changes))


def test_finance_acceptance_journey(client, data, tmp_path, monkeypatch):
    org, actor = data.org.id, data.users["manager"].id
    start, end, label = f.financial_year()
    assert client.get(BASE).status_code == 200
    post(client, BASE + "/budget", dict(year=start.year, category="Repairs", amount="50000", expected_revision="0", revised_date=date.today().isoformat(), description="Approved annual operating budget"))
    post(client, BASE + "/transactions/new", values(amount="100000", kind="INCOME"))
    work = order(data)
    monkeypatch.setattr(documents, "root", lambda org: tmp_path / str(org))
    doc, path = documents.upload(org, actor, FileStorage(stream=io.BytesIO(b"%PDF-1.4 test invoice"), filename="invoice.pdf"),
        dict(access_classification="MEMBERS", title="Member invoice", category="Invoice", effective_date=date.today().isoformat()))
    db.session.commit()
    post(client, BASE + "/commitments", values(amount="15000", provider_id=work.provider_id, work_order_id=work.id, document_id=doc.id))
    c = f.Commitment.query.filter_by(organization_id=org).one()
    initial = f.overview(org, actor)
    assert (initial["cash"], initial["outstanding"], initial["available"]) == (Decimal("100000"), Decimal("15000"), Decimal("85000"))
    for action, kw in [("dispatched", {"dispatch_method":"TELEPHONE"}), ("accepted", {}), ("started", {}), ("completed", {"note":"Legitimate work completed"})]:
        act(data, work, action, **kw)
    db.session.commit()
    assert f.overview(org, actor)["expenditure"] == 0  # Completion is not payment.
    post(client, BASE + "/transactions/new", values(amount="15000", kind="EXPENDITURE", commitment_id=c.id, document_id=doc.id))
    payment = f.Transaction.query.filter_by(organization_id=org, kind="EXPENDITURE").one()
    result = f.overview(org, actor)
    assert (result["expenditure"], result["outstanding"], result["available"]) == (Decimal("15000"), Decimal("0"), Decimal("85000"))
    assert c.status == "PAID" and work.status == "COMPLETED"
    assert payment.provider_id == work.provider_id and payment.work_order_id == work.id
    client.login("resident")
    response = client.get(BASE + f"/transactions/{payment.id}")
    assert b"Streetlight repairs" in response.data and b"R15,000.00" in response.data
    assert b"Internal approved purpose" not in response.data and b"<form" not in response.data
    download = re.search(rb'href="([^"]+/documents/[^"]+/download[^"]*)"', response.data)
    # Follow the actual protected document link, without assuming its path spelling.
    links = re.findall(rb'href="([^"]+)"', response.data)
    link = next(x.decode().replace("&amp;", "&") for x in links if b"/version/" in x or b"/download" in x)
    assert client.get(link).status_code == 200
    assert client.safe_post(BASE + f"/transactions/{payment.id}", dict(operation="visibility", expected_version=payment.version, visibility="PRIVATE")).status_code == 403
    client.login("provider")
    assert client.get(BASE).status_code == 403
    client.login("manager")
    post(client, BASE + "/transactions/new", values(amount="123", visibility="PRIVATE"))
    wrong = f.Transaction.query.filter_by(organization_id=org, amount=Decimal("123")).one()
    post(client, BASE + f"/transactions/{wrong.id}", dict(operation="reverse", transaction_date=date.today().isoformat(), reason="Deliberate incorrect test entry", expected_version=wrong.version, request_key=key()))
    assert db.session.get(f.Transaction, wrong.id) is not None
    assert f.Transaction.query.filter_by(reversal_of_id=wrong.id).count() == 1
    assert f.overview(org, actor)["available"] == Decimal("85000")
    actions = {e.action for e in uip.UipAuditEvent.query.filter_by(organization_id=org).all()}
    assert {"finance.transaction.created", "finance.transaction.reversed", "finance.commitment.created", "finance.commitment.changed", "finance.budget.created"} <= actions


@pytest.mark.parametrize("role,read,write", [("manager",True,True),("owner",True,False),("resident",True,False),("committee_member",True,False),("receptionist",False,False),("provider",False,False),("outsider",False,False)])
def test_finance_role_boundaries(client, data, role, read, write):
    client.login(role)
    for path in ["", "/transactions", "/budget", "/commitments", "/reports/transactions.csv"]:
        assert client.get(BASE + path).status_code == (200 if read else 403)
    result = client.safe_post(BASE + "/transactions/new", values())
    assert result.status_code == (302 if write else 403)


@pytest.mark.parametrize("amount", ["0", "-1", "0.001", "NaN", "Infinity", "1e99999", "1e999999999", "100000000000000.00", "abc"])
def test_invalid_amounts(client, amount):
    assert client.safe_post(BASE + "/transactions/new", values(amount=amount)).status_code == 400


def test_csrf_and_request_dedup(client, data):
    payload = values()
    assert client.post(BASE + "/transactions/new", data=payload).status_code == 400
    post(client, BASE + "/transactions/new", payload)
    assert client.safe_post(BASE + "/transactions/new", payload).status_code == 409
    assert f.Transaction.query.filter_by(organization_id=data.org.id).count() == 1


def test_tenant_idor_and_forged_links(client, data):
    foreign = f.create_transaction(data.other.id, data.outsider.id, values(public_description="OTHER FINANCE"))
    foreign_c = f.create_commitment(data.other.id, data.outsider.id, values())
    db.session.commit()
    assert client.get(BASE + f"/transactions/{foreign.id}").status_code == 404
    assert client.get(BASE + f"/commitments/{foreign_c.id}").status_code == 404
    assert client.safe_post(BASE + "/transactions/new", values(kind="EXPENDITURE", commitment_id=foreign_c.id)).status_code == 404
    for report in ["transactions", "income-expenditure", "budget", "commitments", "category", "provider"]:
        assert b"OTHER FINANCE" not in client.get(BASE + f"/reports/{report}.csv").data
    post(client, BASE + "/transactions/new", values(organization_id=data.other.id))
    assert f.Transaction.query.filter_by(organization_id=data.org.id).count() == 1


def test_private_document_and_private_data_never_published(client, data, tmp_path, monkeypatch):
    org, actor = data.org.id, data.users["manager"].id
    monkeypatch.setattr(documents, "root", lambda org: tmp_path / str(org))
    doc, path = documents.upload(org, actor, FileStorage(stream=io.BytesIO(b"%PDF-secret"), filename="private.pdf"),
        dict(access_classification="PRIVATE", title="SECRET DOCUMENT", category="Invoice", effective_date=date.today().isoformat()))
    row = tx(data, description="SECRET INTERNAL", party="SECRET BANK", document_id=doc.id)
    tx(data, amount="54321", visibility="PRIVATE", public_description="UNPUBLISHED")
    db.session.commit(); client.login("resident")
    html = client.get(BASE + f"/transactions/{row.id}").data
    assert b"SECRET" not in html and b"Supporting document</a>" not in html
    for path in ["", "/transactions", "/reports/transactions.csv", "/reports/provider.csv"]:
        payload = client.get(BASE + path).data
        assert b"SECRET" not in payload and b"UNPUBLISHED" not in payload and b"54321" not in payload
    assert f.overview(org, data.users["resident"].id)["income"] == Decimal("100")


def test_partial_cancel_reversal_and_stale_version(client, data):
    org, actor = data.org.id, data.users["manager"].id
    c = f.create_commitment(org, actor, values(amount="15000"))
    payment = tx(data, kind="EXPENDITURE", amount="5000", commitment_id=c.id)
    assert c.status == "PARTIALLY_PAID" and f.overview(org, actor)["outstanding"] == Decimal("10000")
    db.session.commit()
    assert client.safe_post(BASE + "/transactions/new", values(kind="EXPENDITURE", amount="10001", commitment_id=c.id)).status_code == 409
    assert client.safe_post(BASE + f"/commitments/{c.id}", dict(operation="revise", expected_version=1, amount="20000", reason="stale")).status_code == 409
    post(client, BASE + f"/transactions/{payment.id}", dict(operation="reverse", expected_version=payment.version, request_key=key(), transaction_date=date.today().isoformat(), reason="Correction"))
    assert c.status == "OPEN" and f.paid(org, c.id) == 0
    post(client, BASE + f"/commitments/{c.id}", dict(operation="cancel", expected_version=c.version, reason="No longer needed"))
    assert c.status == "CANCELLED" and f.overview(org, actor)["outstanding"] == 0
    assert client.safe_post(BASE + "/transactions/new", values(kind="EXPENDITURE", commitment_id=c.id)).status_code == 409


def test_budget_revisions_and_year_boundary(client, data):
    org, actor = data.org.id, data.users["manager"].id
    assert f.financial_year(2026) == (date(2026,3,1),date(2027,2,28),"2026/27")
    assert f.financial_year(2027)[1] == date(2028,2,29)
    for d, amount in [("2025-02-28","10"),("2025-03-01","20"),("2026-02-28","30"),("2026-03-01","40")]:
        tx(data, kind="EXPENDITURE", transaction_date=d, amount=amount)
    f.revise_budget(org, actor, dict(year=2025, category="Repairs", amount="100", expected_revision="0", revised_date="2025-03-01", description="Initial"))
    f.revise_budget(org, actor, dict(year=2025, category="Repairs", amount="200", expected_revision="1", revised_date="2025-04-01", description="Revision"))
    r = f.overview(org, actor, 2025)
    assert r["expenditure"] == Decimal("50") and r["annual"] == Decimal("200") and r["remaining"] == Decimal("150")
    assert r["opening"] == Decimal("-10") and r["cash"] == Decimal("-60")
    assert f.BudgetRevision.query.count() == 2
    db.session.commit()
    csv = client.get(BASE + "/reports/transactions.csv?year=2025").data
    assert b"2025-03-01" in csv and b"2026-03-01" not in csv and b"2025-02-28" not in csv


def test_commitment_history_survives_later_cancellation(client, data):
    org, actor = data.org.id, data.users["manager"].id
    c = f.create_commitment(org, actor, values(transaction_date="2025-03-01", amount="15000"))
    f.change_commitment(org, actor, c.id, dict(expected_version=c.version,reason="Cancelled now"),cancel=True)
    assert f.overview(org, actor, 2025)["outstanding"] == Decimal("15000")
    assert f.overview(org, actor)["outstanding"] == 0


def test_visibility_group_and_replacement(client, data):
    org, actor = data.org.id, data.users["manager"].id
    c = f.create_commitment(org, actor, values(visibility="PRIVATE",amount="15000"))
    payment = tx(data, kind="EXPENDITURE", amount="15000", commitment_id=c.id)
    assert not payment.member_visible
    f.set_visibility(org, actor, f.Commitment, c.id, dict(expected_version=c.version,visibility="MEMBERS"))
    assert payment.member_visible and c.member_visible
    reverse = f.reverse_transaction(org, actor, payment.id, dict(expected_version=payment.version,request_key=key(),transaction_date=date.today().isoformat(),reason="Wrong amount"))
    assert reverse.member_visible
    replacement = tx(data,kind="EXPENDITURE",amount="12000",commitment_id=c.id,correction_of_id=payment.id)
    assert replacement.correction_of_id == payment.id and f.overview(org,actor)["outstanding"] == Decimal("3000")
    f.set_visibility(org,actor,f.Transaction,reverse.id,dict(expected_version=reverse.version,visibility="PRIVATE"))
    assert not any(r.member_visible for r in [c,payment,reverse,replacement])
    db.session.commit()
    client.login("owner")
    assert client.get(BASE + f"/transactions/{payment.id}").status_code == 404
    # Existing UIP audit readers do not acquire private financial purpose or amounts.
    audit_rows = uip.UipAuditEvent.query.filter(uip.UipAuditEvent.action.like('finance.%')).all()
    assert all('Wrong amount' not in str(r.metadata_json) and '12000' not in str(r.metadata_json) for r in audit_rows)


def test_model_migration_parity_and_immutability(client, data):
    models = [f.Transaction,f.Commitment,f.BudgetLine,f.BudgetRevision,f.CommitmentRevision]
    inspector = sa.inspect(db.session.connection())
    for model in models:
        columns = inspector.get_columns(model.__tablename__)
        assert {c['name'] for c in columns} == set(model.__table__.columns.keys())
        for c in columns:
            expected = model.__table__.columns[c['name']]
            assert c['nullable'] == expected.nullable
            assert c['type'].compile(dialect=inspector.bind.dialect) == expected.type.compile(dialect=inspector.bind.dialect), (model.__tablename__,c)
        assert {(tuple(c['constrained_columns']), c['referred_table'], tuple(c['referred_columns'])) for c in inspector.get_foreign_keys(model.__tablename__)} == {(tuple(e.parent.name for e in c.elements), c.elements[0].column.table.name, tuple(e.column.name for e in c.elements)) for c in model.__table__.foreign_key_constraints}
        assert {tuple(c['column_names']) for c in inspector.get_unique_constraints(model.__tablename__)} == {tuple(c.columns.keys()) for c in model.__table__.constraints if isinstance(c,sa.UniqueConstraint)}
        assert {c['name'] for c in inspector.get_check_constraints(model.__tablename__)} == {c.name for c in model.__table__.constraints if isinstance(c,sa.CheckConstraint)}
    row=tx(data);db.session.flush()
    with pytest.raises(sa.exc.DBAPIError):
        with db.session.begin_nested():
            db.session.execute(sa.text('UPDATE uip_finance_transaction SET amount=1 WHERE id=:id'),{'id':row.id})
    with pytest.raises(sa.exc.DBAPIError):
        with db.session.begin_nested():
            db.session.execute(sa.text('DELETE FROM uip_finance_transaction WHERE id=:id'),{'id':row.id})


def test_sidebar_active_group_and_palette(client):
    html=client.get(BASE).get_data(as_text=True)
    groups=re.findall(r'<details class="ui-nav-group"([^>]*)><summary>(.*?)</summary>(.*?)</details>',html,re.S)
    assert len(groups)==8
    assert [name for attrs,name,body in groups if 'open' in attrs]==['Finance']
    assert 'aria-current="page"' in next(body for attrs,name,body in groups if name=='Finance')
    for tone in ['blue','green','amber','rose','teal','violet']:
        assert f'class="ui-stat ui-stat-{tone} ui-stat-money"' in html


def test_finance_csv_formula_injection(client,data):
    tx(data,public_description='=HYPERLINK("bad")');db.session.commit();client.login('resident')
    body=client.get(BASE+'/reports/transactions.csv').get_data(as_text=True)
    assert "'=HYPERLINK" in body


# Reuse the existing real overlapping-session harness, adding only Phase 10.
from test_work_order_concurrency import concurrent_db, parallel


@pytest.fixture
def finance_concurrent_db(concurrent_db):
    app, engine, ids = concurrent_db
    with engine.begin() as connection:
        migrate_phase10(connection)
    return app, engine, ids


def test_overlapping_payments_cannot_overpay(finance_concurrent_db):
    app, engine, ids = finance_concurrent_db
    with app.app_context():
        c = f.create_commitment(ids.org, ids.manager, values(amount="15000"))
        cid = c.id
        db.session.commit(); db.session.remove()
    callbacks = [lambda: f.create_transaction(ids.org, ids.manager, values(kind="EXPENDITURE", amount="15000", commitment_id=cid)) for _ in range(2)]
    assert sorted(r[0] for r in parallel(app, callbacks)) == [200,409]
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT sum(amount) FROM uip_finance_transaction").scalar() == Decimal("15000")
        assert connection.exec_driver_sql("SELECT status FROM uip_finance_commitment").scalar() == "PAID"


def test_finance_representative_pages(client,data,tmp_path):
    import os
    from tempfile import gettempdir
    org, actor = data.org.id, data.users["manager"].id
    tx(data,amount="100000")
    f.create_commitment(org,actor,values(amount="15000"))
    f.revise_budget(org,actor,dict(year=f.financial_year()[0].year,category="Repairs",amount="50000",expected_revision="0",revised_date=date.today().isoformat(),description="Operating budget"))
    db.session.commit()
    destination=Path(os.environ.get("UIP_FINANCE_RENDER_DIR",str(tmp_path)))
    assert not (ROOT/'scratch') in destination.resolve().parents
    destination.mkdir(parents=True,exist_ok=True)
    pages={"command-centre":"/dashboard","ratepayers":"/members","properties":"/properties","issues":"/operations/reception","work-orders":"/work-orders","providers":"/providers","meetings":"/operations/meetings","documents":"/operations/documents","settings":"/settings","finance":"/finance","finance-transactions":"/finance/transactions","finance-budget":"/finance/budget","finance-commitments":"/finance/commitments","finance-new":"/finance/transactions/new"}
    for name,path in pages.items():
        result=client.get('/uip/manor-gardens'+path)
        assert result.status_code==200,(name,result.status_code)
        (destination/(name+'.html')).write_text(result.get_data(as_text=True),encoding='utf-8')
    for model, name, route in [(f.Transaction, "finance-transaction-detail", "transactions"), (f.Commitment, "finance-commitment-detail", "commitments")]:
        record = model.query.filter_by(organization_id=org).first()
        response = client.get(BASE + "/" + route + "/" + str(record.id))
        assert response.status_code == 200
        (destination/(name+".html")).write_text(response.get_data(as_text=True), encoding="utf-8")
    client.login('resident')
    (destination/'finance-member.html').write_text(client.get(BASE).get_data(as_text=True),encoding='utf-8')



def test_additive_migration_preserves_existing_local_rows(concurrent_db):
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    app,engine,ids=concurrent_db
    def snapshot(conn,names):
        return {name:sorted(conn.exec_driver_sql('SELECT row_to_json(t)::text FROM "'+name+'" t').scalars().all()) for name in names}
    with engine.begin() as conn:
        names=[n for n in sa.inspect(conn).get_table_names() if n not in {'site_hit','visit_log'}]
        before=snapshot(conn,names)
        migrate_phase10(conn)
        assert snapshot(conn,names)==before
        assert set(sa.inspect(conn).get_table_names())-set(names)=={'uip_finance_transaction','uip_finance_commitment','uip_finance_budget_line','uip_finance_budget_revision','uip_finance_commitment_revision'}
    graph=ScriptDirectory.from_config(Config(str(ROOT/'alembic.ini')))
    assert graph.get_revision('uip_p10_finance').down_revision=='uip_p49_operations'
    assert set(graph.get_heads())=={'uip_p10_finance','7da57fffdba9'}


def test_foreign_provider_and_database_tenant_constraint(client,data):
    from app.uip.services import providers
    foreign=providers.save(data.other.id,data.outsider.id,dict(name='Foreign provider',availability='AVAILABLE'),['SECURITY'])
    db.session.commit()
    assert client.safe_post(BASE+'/transactions/new',values(provider_id=foreign.id)).status_code==404
    row=tx(data)
    db.session.flush()
    with pytest.raises(sa.exc.DBAPIError):
        with db.session.begin_nested():
            db.session.execute(sa.text("INSERT INTO uip_finance_commitment (organization_id,reference,transaction_date,amount,category,description,public_description,public_party,provider_id,member_visible,status,version,recorded_by,request_key) VALUES (:org,'FORGED',CURRENT_DATE,100,'Repairs','Test','Test','',:provider,false,'OPEN',1,:actor,:key)"),dict(org=data.org.id,provider=foreign.id,actor=data.users['manager'].id,key=key()))


def test_adjustment_and_filtered_totals(client,data):
    tx(data,kind='ADJUSTMENT',amount='1000',category='Opening')
    tx(data,kind='ADJUSTMENT',amount='-10',category='Opening')
    tx(data,kind='EXPENDITURE',amount='90',public_party='Published supplier')
    db.session.commit()
    result=f.overview(data.org.id,data.users['manager'].id)
    assert result['adjustment']==Decimal('990') and result['available']==Decimal('900')
    client.login('resident')
    response=client.get(BASE+'/transactions?kind=EXPENDITURE&category=Repairs&party=Published')
    assert b'R90.00' in response.data and b'R990.00' not in response.data


def test_immutable_budget_and_commitment_revisions(client,data):
    org,actor=data.org.id,data.users['manager'].id
    c=f.create_commitment(org,actor,values())
    f.revise_budget(org,actor,dict(year=f.financial_year()[0].year,category='Repairs',amount='100',expected_revision='0',revised_date=date.today().isoformat(),description='Approved'))
    db.session.flush()
    for table in ['uip_finance_budget_revision','uip_finance_commitment_revision','uip_finance_budget_line']:
        with pytest.raises(sa.exc.DBAPIError):
            with db.session.begin_nested():
                db.session.execute(sa.text('DELETE FROM '+table+' WHERE organization_id=:org'),dict(org=org))
