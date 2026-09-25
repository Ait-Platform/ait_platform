"""One focused request-logging test; no database, mail or provider calls."""
import logging
from types import SimpleNamespace
from flask import Flask, g, request
from app.program_uip.log_privacy import install


def test_uip_request_logs_omit_sensitive_contents_and_keep_metadata(caplog):
    app = Flask("uip_log_privacy_test")
    app.add_url_rule("/uip/vote/<org_slug>/<token>", endpoint="uip_bp.public_vote", view_func=lambda **kw: "ok", methods=["GET", "POST"])
    install(app)
    secrets = ["PRIVATE_VOTING_TOKEN", "PRIVATE_QUERY_TOKEN", "PRIVATE_AI_SOURCE", "PRIVATE_API_CREDENTIAL", "PRIVATE_MODEL_SECRET"]
    with caplog.at_level(logging.INFO, logger=app.logger.name):
        with app.test_request_context("/uip/vote/pilot/"+secrets[0]+"?token="+secrets[1], method="POST",
                                      data={"token": secrets[0], "context": secrets[2], "api_key": secrets[3], "model_secret": secrets[4]}):
            g.reqid = "a123b456"
            g.organization = SimpleNamespace(id=42)
            g.org_id = 42
            request.user_id = 7
            app.logger.info("[%s] %s %s ep=%s args=%s form=%s user_id=%s",g.reqid,request.method,request.path,request.endpoint,dict(request.args),dict(request.form),7)
            app.logger.info("AI source and credentials: %s",secrets)
            app.logger.info("[%s] response %s redirect to %s",g.reqid,"302 FOUND","/vote#"+secrets[0])
            try:
                raise ValueError(secrets[3])
            except ValueError:
                app.logger.exception("Provider failure: %s",secrets[2])
        with app.test_request_context("/uip/vote/pilot/"+secrets[0],method="POST",json={"prompt":secrets[2],"key":secrets[3]}):
            app.logger.info("body=%s",request.get_json())
        # Unmatched UIP URLs must not leak token-shaped path/query values either.
        with app.test_request_context("/uip/unknown/"+secrets[0]):
            app.logger.warning("Unknown path %s",request.path)
    assert not any(secret in caplog.text for secret in secrets)
    for safe in ("method=POST", "route=/uip/vote/<org_slug>/<token>", "endpoint=uip_bp.public_vote", "status=302", "organization_id=42", "user_id=7", "request_id=a123b456"):
        assert safe in caplog.text
    assert any(r.levelno==logging.ERROR for r in caplog.records)
    assert "exception_type=ValueError" in caplog.text
    assert all(r.exc_info is None and r.exc_text is None and r.stack_info is None for r in caplog.records)
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=app.logger.name):
        with app.test_request_context("/other-product"):
            app.logger.info("form=%s",{"test":"other-product-marker"})
    assert "other-product-marker" in caplog.text


def test_uip_error_logs_safe_application_location_without_exception_values(caplog):
    from pathlib import Path
    app = Flask("uip_error_location_test")
    app.config.update(TESTING=False, PROPAGATE_EXCEPTIONS=False)
    secret = "PRIVATE_DATABASE_VALUE"
    source_path = Path(__file__).resolve().parents[2] / "app/program_uip/diagnostic_fixture.py"
    namespace = {"secret": secret}
    # A synthetic application frame, without creating or importing a source file.
    exec(compile("def fail():\n    raise RuntimeError(secret)\n", str(source_path), "exec"), namespace)
    app.add_url_rule("/uip/example/router", endpoint="uip_bp.router_page", view_func=namespace["fail"])
    install(app)
    with caplog.at_level(logging.ERROR, logger=app.logger.name):
        response = app.test_client().get("/uip/example/router")
    assert response.status_code == 500
    assert "exception_type=RuntimeError" in caplog.text
    assert "app/program_uip/diagnostic_fixture.py:2" in caplog.text
    assert secret not in caplog.text
    assert str(source_path.parent.parent.parent) not in caplog.text
    assert "raise RuntimeError" not in caplog.text
    assert all(r.exc_info is None and r.exc_text is None and r.stack_info is None for r in caplog.records)
