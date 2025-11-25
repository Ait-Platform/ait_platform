from datetime import datetime
from flask import current_app, url_for, request
from sqlalchemy import text
from app.extensions import db


def handle_exit_actions(user_id: int, subject_slug: str, run_id: int | None = None, email: str | None = None):
    """
    - Marks user_enrollment completed + email_status 'pending'
    - Builds artifact URL
    - Sends email via known-good async mailer
    - Updates user_enrollment: email_status/emailed_at/report_pdf_url
    Returns: {'status': 'ok'|'fail', 'artifact_url': str|None}
    """
    try:
        # 1) Resolve subject id
        sid = db.session.execute(
            text("SELECT id FROM auth_subject WHERE lower(slug)=:s LIMIT 1"),
            {"s": (subject_slug or "").lower()},
        ).scalar()

        if not sid:
            current_app.logger.warning("[exit] subject %r not found", subject_slug)
            return {"status": "fail", "artifact_url": None}

        # 2) Load DB email; prefer finish-form email if provided
        row = db.session.execute(
            text('SELECT email FROM "user" WHERE id=:uid LIMIT 1'),
            {"uid": int(user_id)},
        ).mappings().first()

        db_email = (row["email"] or "").strip().lower() if row else ""
        to_email = (email or "").strip().lower() or db_email

        if not to_email:
            # mark fail clearly; do not leave 'pending'
            db.session.execute(
                text("""
                    UPDATE user_enrollment
                       SET email_status='fail', email_error='no email available'
                     WHERE user_id=:uid AND subject_id=:sid
                """),
                {"uid": int(user_id), "sid": int(sid)},
            )
            db.session.commit()
            return {"status": "fail", "artifact_url": None}

        # 3) Mark completed now, set email_status pending
        db.session.execute(
            text("""
                UPDATE user_enrollment
                   SET status='completed',
                       completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
                       email_status = 'pending'
                 WHERE user_id=:uid AND subject_id=:sid
            """),
            {"uid": int(user_id), "sid": int(sid)},
        )
        db.session.commit()

        # 4) Build artifact URL (subject-specific)
        artifact_url: str | None = None

        if subject_slug == "loss":
            try:
                from app.subject_loss.routes import _build_loss_pdf_and_get_url
                artifact_url = _build_loss_pdf_and_get_url(
                    run_id=run_id,
                    user_id=user_id,
                )
            except Exception as e:
                current_app.logger.exception(
                    "handle_exit_actions: loss PDF build failed: %s", e
                )
                # fallback: plain PDF endpoint
                artifact_url = url_for(
                    "loss_bp.report_pdf",
                    run_id=run_id,
                    user_id=user_id,
                    _external=True,
                )

        elif subject_slug == "reading":
            try:
                from app.subject_reading.routes import build_certificate_url  # adjust if needed
                artifact_url = build_certificate_url(user_id=user_id)
            except Exception as e:
                current_app.logger.exception(
                    "handle_exit_actions: reading certificate build failed: %s", e
                )
                artifact_url = None

        # 5) Normalize artifact_url into a full https:// URL for emails
        base_url = (current_app.config.get("SITE_BASE_URL") or "").rstrip("/")
        if not base_url:
            # fallback for dev / misconfig
            base_url = (request.url_root or "").rstrip("/")

        if artifact_url:
            if artifact_url.startswith("http://") or artifact_url.startswith("https://"):
                email_url = artifact_url
            else:
                email_url = f"{base_url}{artifact_url}"
        else:
            email_url = None

        # 6) Send email via known-good async mailers
        try:
            if subject_slug == "loss":
                from app.subject_loss.routes import _send_loss_report_email_async

                _send_loss_report_email_async(
                    to_email=to_email,
                    run_id=run_id,
                    user_id=user_id,
                    pdf_url=email_url,
                )

            elif subject_slug == "reading":
                from app.utils.mailer import send_certificate_email_async  # adjust if needed

                send_certificate_email_async(
                    to_email=to_email,
                    user_id=user_id,
                    certificate_url=email_url,
                )

            # 7) Mark OK
            db.session.execute(
                text("""
                    UPDATE user_enrollment
                       SET email_status='ok',
                           emailed_at=:t,
                           report_pdf_url=:u
                     WHERE user_id=:uid AND subject_id=:sid
                """),
                {
                    "uid": int(user_id),
                    "sid": int(sid),
                    "u": email_url,
                    "t": datetime.utcnow(),
                },
            )
            db.session.commit()
            return {"status": "ok", "artifact_url": email_url}

        except Exception as e:
            db.session.rollback()
            current_app.logger.exception("handle_exit_actions: email send failed: %s", e)

            db.session.execute(
                text("""
                    UPDATE user_enrollment
                       SET email_status='fail',
                           email_error=:err,
                           report_pdf_url=COALESCE(report_pdf_url, :u)
                     WHERE user_id=:uid AND subject_id=:sid
                """),
                {
                    "uid": int(user_id),
                    "sid": int(sid),
                    "u": email_url,
                    "err": str(e)[:300],
                },
            )
            db.session.commit()
            return {"status": "fail", "artifact_url": email_url}

    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("handle_exit_actions failed: %s", e)
        return {"status": "fail", "artifact_url": None}
