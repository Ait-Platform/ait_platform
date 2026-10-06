"""Operator-only provisioning entry issuance; no public self-grant endpoint."""
import click
from . import home_sace_bp
from .service import issue_provisioning
from app.extensions import db


@home_sace_bp.cli.command("provision-link")
@click.option("--email", required=True)
@click.option("--issued-by", required=True)
def provision_link(email, issued_by):
    """Create one HOME-only, email-bound provisioning link (seven days)."""
    if "@" not in email or not issued_by.strip():
        raise click.ClickException("Provide the controller email and issuing operator identity.")
    token = issue_provisioning(email, issued_by)
    db.session.commit()
    click.echo("/sace/home/provisioning?token=" + token)


@home_sace_bp.cli.command("publish-document")
@click.option("--controller-user-id", type=int, required=True)
@click.option("--kind", required=True)
@click.option("--version", required=True)
@click.option("--storage-key", required=True)
@click.option("--source-file", type=click.Path(exists=True, dir_okay=False), default=None,
              help="Approved PDF to stage privately and store using its exact key.")
@click.option("--manifest", type=click.Path(exists=True, dir_okay=False), required=True)
def publish_document_command(controller_user_id, kind, version, storage_key, manifest, source_file):
    """Register an already-approved PDF in the private HOME document root."""
    import json
    from pathlib import Path
    from app.models.sace_home import HomeController
    from .service import publish_document
    owner = HomeController.query.filter_by(user_id=controller_user_id, active=True).first()
    if owner is None:
        raise click.ClickException("An active HOME controller is required.")
    try:
        row = publish_document(owner, kind, version, storage_key,
            json.loads(Path(manifest).read_text(encoding="utf-8")),
            content=Path(source_file).read_bytes() if source_file else None)
        db.session.commit()
    except (ValueError, OSError) as exc:
        db.session.rollback()
        raise click.ClickException(str(exc)) from exc
    click.echo("Published HOME document version " + str(row.id))


@home_sace_bp.cli.command('publish-endorsement-documents')
@click.option('--controller-user-id', type=int, required=True)
@click.option('--source-directory', type=click.Path(exists=True, file_okay=False), required=True)
@click.option('--approved-by', required=True)
@click.option('--approval-reference', required=True)
def publish_endorsement_documents(controller_user_id, source_directory, approved_by, approval_reference):
    """Copy and register the three approved HOME PDFs; reuse matching versions."""
    from app.models.sace_home import HomeController
    from .endorsement_documents import publish
    owner = HomeController.query.filter_by(user_id=controller_user_id, active=True).first()
    if owner is None:
        raise click.ClickException('An active HOME controller is required.')
    try:
        rows = publish(owner, source_directory, approved_by, approval_reference)
        db.session.commit()
    except (ValueError, OSError) as exc:
        db.session.rollback()
        raise click.ClickException(str(exc)) from exc
    for kind, row in rows.items():
        click.echo(kind + ': HOME document version ' + str(row.id))
