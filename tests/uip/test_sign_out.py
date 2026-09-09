"""Focused shell/logout check without the shared factory or any database."""
import ast
from pathlib import Path
from types import SimpleNamespace
from flask import Flask, render_template, redirect, session, url_for
from flask_login import LoginManager, UserMixin, logout_user, login_required
from flask_wtf import CSRFProtect
from playwright.sync_api import sync_playwright


def test_uip_sign_out_reuses_ait_logout_on_desktop_mobile_and_public_vote():
    root=Path(__file__).resolve().parents[2]
    app=Flask("uip_logout_check",template_folder=str(root/"templates"))
    app.config.update(TESTING=True,SECRET_KEY="isolated-sign-out-check-only")
    manager=LoginManager(app); CSRFProtect(app)
    class Account(UserMixin): id="42"
    manager.user_loader(lambda uid:Account() if uid=="42" else None)
    # Run the exact existing AIT logout function, without importing its factory,
    # unrelated routes or models. No alternate logout implementation is tested.
    tree=ast.parse((root/"app/auth/routes.py").read_text(encoding="utf-8-sig"))
    logout=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=="logout")
    logout.decorator_list=[]
    scope=dict(logout_user=logout_user,session=session,redirect=redirect,url_for=url_for)
    exec(compile(ast.Module(body=[logout],type_ignores=[]),"existing_ait_logout","exec"),scope)
    app.add_url_rule("/logout",endpoint="auth_bp.logout",view_func=scope["logout"],methods=["GET","POST"])
    app.add_url_rule("/",endpoint="public_bp.welcome",view_func=lambda:"Home")
    for endpoint in ("dashboard","help_page","ai_assistant"):
        app.add_url_rule("/uip/<org_slug>/"+endpoint,endpoint="uip_bp."+endpoint,view_func=lambda org_slug:"UIP")
    context=dict(org=SimpleNamespace(name="Manor Gardens UIP",slug="manor-gardens"),uip_roles={"provider"},
                 uip_nav_groups={"Service Providers":[dict(href="/work-orders",label="Work Orders",active=True)]},uip_help_anchor="service-providers")
    @app.route("/shell")
    @login_required
    def shell(): return render_template("uip/base.html",**context)
    @app.route("/vote")
    def vote(): return render_template("uip/public_vote.html",org=context["org"],survey=None,done=False,token="")
    client=app.test_client()
    assert b"Sign out" not in client.get("/vote").data
    with client.session_transaction() as state:
        state["_user_id"]="42";state["_fresh"]=True;state["selected_org"]=1
    client.set_cookie("remember_token","synthetic-remember-cookie")
    response=client.get("/shell")
    assert response.status_code==200
    html=response.get_data(as_text=True)
    assert 'href="/logout">Sign out</a>' in html
    assert 'href="/logout">Sign out</a>' in client.get("/vote").get_data(as_text=True)
    assert "AI assistant</a>" not in html and "AI &amp; Wallet" not in html
    assert html.count('class="ui-nav-group"')==1 and "Work Orders" in html
    with sync_playwright() as p:
        browser=p.chromium.launch()
        for width in (1440,390):
            page=browser.new_page(viewport={"width":width,"height":900})
            page.route("**/*",lambda route:route.abort())
            page.set_content(html)
            link=page.get_by_role("link",name="Sign out",exact=True)
            assert link.is_visible()
            assert page.locator(".ui-sidebar-account a").count()==1
            assert page.locator(".ui-topbar").get_by_role("link",name="Sign out",exact=True).count()==0
            if width==390:
                assert not page.locator(".ui-mobile-menu").evaluate("element => element.open")
            box=link.bounding_box()
            assert box and box["x"]>=0 and box["x"]+box["width"]<=width and box["y"]+box["height"]<=900
            assert page.evaluate("document.documentElement.scrollWidth<=innerWidth")
            page.close()
        browser.close()
    result=client.get("/logout")
    assert result.status_code==302 and result.headers["Location"]=="/"
    with client.session_transaction() as state: assert not dict(state)
    assert client.get_cookie("remember_token") is None
    assert client.get("/shell").status_code==401
    assert b"Sign out" not in client.get("/vote").data
