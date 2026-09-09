"""Focused real-rendered intake search, selection and register authority checks."""
from datetime import date, timedelta
from playwright.sync_api import sync_playwright
from bootstrap import db, core, uip
from app.uip.services import register
from test_register import BASE, MEMBER, PROPERTY, make_member, make_property


def test_register_intake_search_selection_and_autofill(client, data):
    today = date.today()
    member = make_member(data, name="Thabo Mkhize", phone="082 555 0101")
    multiple = make_member(data, reference="M2", name="Naledi Dlamini", email="", phone="")
    historical = make_member(data, reference="M3", name="Historical Member", email="")
    properties = [make_property(data, reference=f"P{i}", address=f"{i} Mock Garden Lane", rates_reference=f"RATES{i}") for i in range(1, 5)]
    db.session.flush()
    def link(m, p, start, end=None, kind="owner"):
        db.session.add(uip.UipPropertyMember(organization_id=data.org.id, member_id=m.id,
            property_id=p.id, relationship=kind, valid_from=start, valid_to=end, is_verified=True))
    link(member, properties[0], today)
    link(member, properties[0], today, kind="occupier")  # One property, two current roles.
    link(member, properties[1], today-timedelta(days=10), today-timedelta(days=1))
    link(member, properties[2], today+timedelta(days=1))
    link(multiple, properties[1], today)
    link(multiple, properties[2], today, today)  # End date remains inclusive.
    link(historical, properties[3], today-timedelta(days=10), today-timedelta(days=1))
    outsider = register.save_member(data.other.id, data.outsider.id, {**MEMBER, "reference":"SECRET-OTHER", "name":"Other org secret"})
    register.save_property(data.other.id, data.outsider.id, {**PROPERTY, "address":"Other org private address"})
    db.session.commit()
    counts = (uip.UipMemberProfile.query.count(), uip.UipProperty.query.count(), core.CoreOrganizationMember.query.count(), uip.UipPropertyMember.query.count())
    response = client.get(BASE + "/interaction/new")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "SECRET-OTHER" not in html and "Other org private address" not in html
    assert client.get(BASE + f"/interaction/new?member_id={outsider.id}").status_code == 404
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        for width in (1440, 390):
            page = browser.new_page(viewport={"width":width,"height":1000})
            page.route("**/*", lambda route: route.abort())
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.set_content(html)
            search = page.locator("#member-search")
            select = page.locator("#member_id")
            for query in ("thab", "mkh", "M1", "person@", "082555", "1 mock garden", "rates1"):
                search.fill(query)
                assert select.locator(f'option[value="{member.id}"]').count() == 1, query
                assert select.locator('option[value=""]').count() == 1
            search.fill("no such ratepayer")
            assert select.locator("option").count() == 1
            search.fill("thab")
            select.select_option(str(member.id))
            assert search.input_value() == "Thabo Mkhize / M1"
            assert "Thabo Mkhize" in page.locator("#member-selection").inner_text()
            assert page.locator("#property_id").input_value() == str(properties[0].id)
            summary = page.locator("#intake-register-summary").inner_text()
            for value in ("082 555 0101", "person@example.invalid", "RATES1", "owner (verified)", "occupier (verified)"):
                assert value in summary
            search.fill("nal")
            assert select.input_value() == str(member.id)  # Searching does not silently change selection.
            select.select_option(str(multiple.id))
            assert page.locator("#property_id").input_value() == ""
            assert page.locator('#property_id optgroup[label="Current linked properties"] option').count() == 2
            page.locator("#property_id").select_option(str(properties[2].id))
            assert "RATES3" in page.locator("#intake-register-summary").inner_text()
            assert "Missing from register" in page.locator("#intake-register-summary").inner_text()
            search.fill("histor")
            select.select_option(str(historical.id))
            assert page.locator("#property_id").input_value() == ""
            assert page.locator('#property_id optgroup[label="Current linked properties"]').count() == 0
            page.locator("#general-enquiry").click()
            assert select.input_value() == "" and search.input_value() == ""
            page.locator("#property-search").fill("rates4")
            assert page.locator('#property_id option').count() == 2
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            assert not errors
            page.close()
        # A register-profile shortcut also auto-selects its sole current property.
        page = browser.new_page()
        page.route("**/*", lambda route: route.abort())
        page.set_content(client.get(BASE + f"/interaction/new?member_id={member.id}").get_data(as_text=True))
        assert page.locator("#property_id").input_value() == str(properties[0].id)
        browser.close()
    assert client.safe_post(BASE + "/interaction/new", dict(title="Register intake", description="Follow up",
        category="SECURITY", channel="Telephone", priority="NORMAL", member_id=member.id,
        property_id=properties[0].id, email="invented@example.invalid", phone="fake")).status_code == 302
    issue = core.CoreInteraction.query.filter_by(title="Register intake").one()
    assert (issue.member_id, issue.property_id) == (member.id, properties[0].id)
    assert member.email == "person@example.invalid" and member.phone == "082 555 0101"
    assert counts == (uip.UipMemberProfile.query.count(), uip.UipProperty.query.count(), core.CoreOrganizationMember.query.count(), uip.UipPropertyMember.query.count())
