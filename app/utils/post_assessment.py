from flask import current_app
from sqlalchemy import text
from datetime import datetime
from app.extensions import db

# app/utils/post_assessment.py
from datetime import datetime
from flask import current_app, url_for
from sqlalchemy import text
from app import db

from datetime import datetime
from flask import current_app, url_for
from sqlalchemy import text
from app import db

def handle_exit_actions(user_id: int, subject_slug: str, run_id: int | None = None, email: str | None = None):
    """
    - Marks user_enrollment completed + email_status 'pending'
    - Builds artifact URL
    - Sends email via known-good async mailer
    - Updates user_enrollment: email_status/emailed_at/report_pdf_url
    Returns: {'status': 'ok'|'fail', 'artifact_url': str|None}
    """
    try:
        # Resolve subject id
        sid = db.session.execute(
            text("SELECT id FROM auth_subject WHERE lower(slug)=:s LIMIT 1"),
            {"s": (subject_slug or '').lower()},
        ).scalar()
        if not sid:
            current_app.logger.warning(f"[exit] subject {subject_slug!r} not found")
            return {"status": "fail", "artifact_url": None}

        # Load DB email; prefer finish-form email if provided; persist if changed
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

        # ...unchanged up to to_email calculation...


        db_email = (row["email"] or "").strip().lower() if row else ""
        to_email = (email or "").strip().lower() or db_email
        if not to_email:
            db.session.execute(text("""
                UPDATE user_enrollment
                SET email_status='fail', email_error='no email available'
                WHERE user_id=:uid AND subject_id=:sid
            """), {"uid": int(user_id), "sid": int(sid)})
            db.session.commit()
            return {"status": "fail", "artifact_url": None}

        # ⬇️ REMOVE the user.email update block that was here

        # then continue with:
        # - mark completed + email_status='pending'
        # - build artifact_url
        # - send async email
        # - set email_status='ok', emailed_at, report_pdf_url


        # Mark completed now, set email_status pending
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

        # Build artifact URL
        artifact_url = None
        if subject_slug == "loss":
            try:
                from app.subject_loss.routes import _build_loss_pdf_and_get_url
                artifact_url = _build_loss_pdf_and_get_url(run_id=run_id, user_id=user_id)
            except Exception as e:
                current_app.logger.exception("handle_exit_actions: loss PDF build failed: %s", e)
                artifact_url = url_for("loss_bp.report_pdf", run_id=run_id, user_id=user_id, _external=True)
        elif subject_slug == "reading":
            try:
                from app.subject_reading.routes import build_certificate_url  # adjust if different
                artifact_url = build_certificate_url(user_id=user_id)
            except Exception:
                artifact_url = None

        # Send email via known-good async mailers
        try:
            if subject_slug == "loss":
                from app.subject_loss.routes import _send_loss_report_email_async
                _send_loss_report_email_async(
                    to_email=to_email,
                    run_id=run_id,
                    user_id=user_id,
                    pdf_url=artifact_url,
                )
            elif subject_slug == "reading":
                from app.utils.mailer import send_certificate_email_async  # adjust if needed
                send_certificate_email_async(
                    to_email=to_email,
                    user_id=user_id,
                    certificate_url=artifact_url,
                )

            # Mark OK
            db.session.execute(
                text("""
                    UPDATE user_enrollment
                       SET email_status='ok',
                           emailed_at=:t,
                           report_pdf_url=:u
                     WHERE user_id=:uid AND subject_id=:sid
                """),
                {"uid": int(user_id), "sid": int(sid), "u": artifact_url, "t": datetime.utcnow()},
            )
            db.session.commit()
            return {"status": "ok", "artifact_url": artifact_url}

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
                {"uid": int(user_id), "sid": int(sid), "u": artifact_url, "err": str(e)[:300]},
            )
            db.session.commit()
            return {"status": "fail", "artifact_url": artifact_url}

    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("handle_exit_actions failed: %s", e)
        return {"status": "fail", "artifact_url": None}

