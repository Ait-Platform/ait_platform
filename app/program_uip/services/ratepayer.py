"""Read-only Vault verification and personally attributable RP queries."""
from datetime import date
from hashlib import sha256
from io import BytesIO
from uuid import uuid4
from flask import abort, current_app, send_file
from sqlalchemy import func, or_
from werkzeug.utils import secure_filename
from app.extensions import db
from app.models.core import CoreInteraction, CoreAuditEvent
from app.models.uip import UipMemberProfile, UipProperty, UipPropertyMember, UipRegisterImport, UipDocument
from app.models.uip_operations import UipDocumentVersion

PHOTO_PREFIX = "uip/rp_queries"


def vault_identity(org, user):
    """Roles alone never prove RP/property authority; navigation never writes it."""
    batches = UipRegisterImport.query.filter_by(organization_id=org, source_type="MUNICIPAL").filter(
        UipRegisterImport.status.in_(("COMPLETED", "WITH_EXCEPTIONS")),
        UipRegisterImport.effective_date <= date.today()).all()
    ids = [batch.id for batch in batches]
    if not user.is_active or not ids or not (user.email or "").strip():
        return None, [], bool(ids)
    members = UipMemberProfile.query.filter(
        UipMemberProfile.organization_id == org, UipMemberProfile.is_active.is_(True),
        UipMemberProfile.record_source == "MUNICIPAL", UipMemberProfile.last_import_id.in_(ids),
        UipMemberProfile.eligibility_status != "ineligible",
        func.lower(func.trim(UipMemberProfile.email)) == user.email.strip().lower()).all()
    # Ambiguous municipal identities must not disclose another person's property.
    if len(members) != 1:
        return None, [], True
    member = members[0]
    properties = UipProperty.query.join(UipPropertyMember,
        (UipPropertyMember.property_id == UipProperty.id) &
        (UipPropertyMember.organization_id == UipProperty.organization_id)).filter(
        UipProperty.organization_id == org, UipProperty.is_active.is_(True),
        UipProperty.record_source == "MUNICIPAL", UipProperty.last_import_id.in_(ids),
        UipPropertyMember.member_id == member.id, UipPropertyMember.relationship == "owner",
        UipPropertyMember.is_verified.is_(True), UipPropertyMember.record_source == "MUNICIPAL",
        UipPropertyMember.last_import_id.in_(ids), UipPropertyMember.valid_from <= date.today(),
        or_(UipPropertyMember.valid_to.is_(None), UipPropertyMember.valid_to >= date.today())
    ).distinct().all()
    return member, properties, True


def lodge_query(org, user, member, values, photo):
    title = (values.get("title") or "").strip()
    description = (values.get("description") or "").strip()
    if not title or len(title) > 255 or not description or len(description) > 10000:
        abort(400, description="Provide a title (up to 255 characters) and details (up to 10000 characters).")
    content = None
    if photo and photo.filename:
        filename = secure_filename(photo.filename)
        extension = filename.rsplit(".", 1)[-1].lower()
        types = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png"}
        limit = min(int(current_app.config.get("UIP_RP_PHOTO_MAX_BYTES", 5 * 1024 * 1024)), 10 * 1024 * 1024)
        content = photo.stream.read(limit + 1)
        if not filename or len(filename) > 255 or extension not in types or not content or len(content) > limit:
            abort(400, description="Select a JPEG or PNG photograph within the upload limit.")
        # Decode the image to reject disguised files; no disk copy is made.
        from PIL import Image, UnidentifiedImageError
        try:
            with Image.open(BytesIO(content)) as image:
                if image.format != ("PNG" if extension == "png" else "JPEG"):
                    raise ValueError()
                image.verify()
        except (ValueError, OSError, UnidentifiedImageError, Image.DecompressionBombError):
            abort(400, description="The photograph could not be read as a valid image.")
        photo.stream = BytesIO(content)
        photo.headers["Content-Type"] = types[extension]
    query = CoreInteraction(organization_id=org, creator_id=user.id, recorded_by=user.id,
        member_id=member.id, reference="RP-" + uuid4().hex, channel="Web",
        interaction_type="municipal_fault", category="Service delivery", title=title,
        description=description, status="NEW")
    db.session.add(query)
    db.session.flush()
    if content is not None:
        from app.utils.cloudflare_r2 import upload_file_to_r2
        try:
            key = upload_file_to_r2(photo, prefix=PHOTO_PREFIX, return_key=True)
        except Exception:
            db.session.rollback()
            abort(503, description="The photograph could not be stored. Please retry your query.")
        document = UipDocument(organization_id=org, uploader_id=user.id, interaction_id=query.id,
            filename=filename, file_type=extension, title="Query photograph", category="RP_QUERY_PHOTO",
            access_classification="PRIVATE", current_version=1)
        db.session.add(document)
        db.session.flush()
        db.session.add(UipDocumentVersion(organization_id=org, document_id=document.id, version=1,
            effective_date=date.today(), actor_user_id=user.id, filename=filename, storage_key=key,
            content_type=types[extension], size_bytes=len(content), sha256=sha256(content).hexdigest(),
            replacement_reason="Initial RP query photograph"))
    db.session.add(CoreAuditEvent(organization_id=org, user_id=user.id, action="RP_QUERY_CREATED",
        entity_type="CoreInteraction", entity_id=query.id))
    return query


def photo_response(org, user, document_id):
    member, properties, available = vault_identity(org, user)
    if not member or not properties:
        abort(403)
    document = UipDocument.query.join(CoreInteraction, CoreInteraction.id == UipDocument.interaction_id).filter(
        UipDocument.id == document_id, UipDocument.organization_id == org,
        UipDocument.uploader_id == user.id, UipDocument.category == "RP_QUERY_PHOTO",
        CoreInteraction.organization_id == org, CoreInteraction.creator_id == user.id,
        CoreInteraction.interaction_type == "municipal_fault").first_or_404()
    version = UipDocumentVersion.query.filter_by(organization_id=org,
        document_id=document.id, version=1).first_or_404()
    if not version.storage_key.startswith(PHOTO_PREFIX + "/"):
        abort(404)
    from app.utils.cloudflare_r2 import read_file_from_r2
    try:
        content = read_file_from_r2(version.storage_key)
    except Exception:
        abort(503, description="The photograph is temporarily unavailable.")
    response = send_file(BytesIO(content), mimetype=version.content_type, download_name=version.filename)
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response
