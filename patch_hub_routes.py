import re

filepath = 'app/program_uip/completion_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''    kind = request.form.get("kind", "members")
    rows, token, error, summary = [], None, None, None
    if request.method == "POST":
        upload = request.files.get("file")'''

replace_code = '''    kind = request.form.get("kind", "members")
    
    # Handle Start Batch Action
    if request.method == "POST" and request.form.get("operation") == "start_batch":
        session['vault_batch_ref'] = request.form.get("batch_reference")
        session['vault_source'] = request.form.get("source_identifier")
        session['vault_date'] = request.form.get("effective_date")
        flash("Vault batch initialized. You may now begin uploading tables.", "success")
        return redirect(url_for("uip_bp.mo_vault_import" if is_mo_vault else "uip_bp.register_import", org_slug=org_slug))

    if request.method == "POST" and request.form.get("operation") == "reset_batch":
        session.pop('vault_batch_ref', None)
        session.pop('vault_source', None)
        session.pop('vault_date', None)
        return redirect(url_for("uip_bp.mo_vault_import" if is_mo_vault else "uip_bp.register_import", org_slug=org_slug))

    rows, token, error, summary = [], None, None, None
    if request.method == "POST" and request.form.get("operation") in ["preview", "commit"]:
        upload = request.files.get("file")'''

content = content.replace(search_code, replace_code)

search_code2 = '''            metadata = {
                "source_identifier": request.form.get("source_identifier"),
                "batch_reference": request.form.get("batch_reference"),
                "date_received": datetime.utcnow().date(),
                "effective_date": datetime.strptime(request.form.get("effective_date", datetime.utcnow().strftime("%Y-%m-%d")), "%Y-%m-%d").date(),
                "document_id": doc.id
            }'''

replace_code2 = '''            metadata = {
                "source_identifier": session.get("vault_source", request.form.get("source_identifier")),
                "batch_reference": session.get("vault_batch_ref", request.form.get("batch_reference")),
                "date_received": datetime.utcnow().date(),
                "effective_date": datetime.strptime(session.get("vault_date", request.form.get("effective_date", datetime.utcnow().strftime("%Y-%m-%d"))), "%Y-%m-%d").date(),
                "document_id": doc.id
            }'''

content = content.replace(search_code2, replace_code2)

search_code3 = '''    return render_template("program_uip/register_import.html", org=g.organization, columns=CSV_COLUMNS,
                           kind=kind, rows=rows, preview_token=token, summary=summary, error=error), (400 if error else 200)'''

replace_code3 = '''    from app.models.uip import UipRegisterImport
    import_status = {"members": None, "properties": None, "relationships": None}
    
    batch_ref = session.get('vault_batch_ref')
    if batch_ref:
        # Check what has been successfully imported in this batch
        imports = UipRegisterImport.query.filter_by(organization_id=g.organization.id, batch_reference=batch_ref, status="COMPLETED").all()
        for imp in imports:
            if imp.notes in import_status:
                import_status[imp.notes] = imp

    return render_template("program_uip/register_import.html", org=g.organization, columns=CSV_COLUMNS,
                           kind=kind, rows=rows, preview_token=token, summary=summary, error=error,
                           import_status=import_status, 
                           vault_batch_ref=session.get('vault_batch_ref'),
                           vault_source=session.get('vault_source'),
                           vault_date=session.get('vault_date')), (400 if error else 200)'''

content = content.replace(search_code3, replace_code3)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
