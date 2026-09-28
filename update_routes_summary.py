import re

filepath = 'app/program_uip/completion_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search = '''    batch_ref = session.get('vault_batch_ref')
    if batch_ref:
        # Check what has been successfully imported in this batch
        imports = UipRegisterImport.query.filter(
            UipRegisterImport.organization_id == g.organization.id,
            UipRegisterImport.batch_reference == batch_ref,
            UipRegisterImport.status.in_(["COMPLETED", "WITH_EXCEPTIONS"])
        ).all()
        for imp in imports:
            if imp.notes in import_status:
                import_status[imp.notes] = imp

    return render_template("program_uip/register_import.html", org=g.organization, columns=CSV_COLUMNS,
                           kind=kind, rows=rows, preview_token=token, summary=summary, error=error,
                           import_status=import_status, 
                           vault_batch_ref=session.get('vault_batch_ref'),
                           vault_source=session.get('vault_source'),
                           vault_date=session.get('vault_date')), (400 if error else 200)'''

replace = '''    
    # Fetch the most recent batch to show the summary table
    latest_batch = UipRegisterImport.query.filter_by(
        organization_id=g.organization.id,
        imported_by_user_id=current_user.id,
        notes="master_roll"
    ).order_by(UipRegisterImport.id.desc()).first()
    
    latest_exceptions = []
    if latest_batch and latest_batch.status == "WITH_EXCEPTIONS":
        from app.models.uip import UipRegisterImportException
        latest_exceptions = UipRegisterImportException.query.filter_by(import_id=latest_batch.id).all()

    return render_template("program_uip/register_import.html", org=g.organization, columns=CSV_COLUMNS,
                           kind=kind, rows=rows, preview_token=token, summary=summary, error=error,
                           latest_batch=latest_batch, latest_exceptions=latest_exceptions), (400 if error else 200)'''

content = content.replace(search, replace)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated routes to fetch latest batch")
