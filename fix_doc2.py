import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# I will use regex to catch the UipDocument initialization
old_doc_pattern = re.compile(r'doc_record = UipDocument\(\s*organization_id=org\.id,\s*resolution_id=res\.id,\s*title=f"Proof: \{res\.title\}",\s*category="MANDATE",\s*access_classification="public",\s*uploaded_by=current_user\.id,\s*effective_date=datetime\.utcnow\(\)\.date\(\),\s*filename=filename,\s*size_bytes=0,\s*current_version=1\s*\)\s*db\.session\.add\(doc_record\)\s*db\.session\.flush\(\)\s*file_path = os\.path\.join\(upload_dir, f"\{doc_record\.id\}_\{filename\}"\)\s*f\.save\(file_path\)\s*doc_record\.size_bytes = os\.path\.getsize\(file_path\)', re.DOTALL)

new_doc_logic = '''doc_record = UipDocument(
                    organization_id=org.id,
                    uploader_id=current_user.id,
                    title=f"Proof: {res.title}",
                    category="MANDATE",
                    access_classification="PUBLIC",
                    filename=filename,
                    current_version=1
                )
                db.session.add(doc_record)
                db.session.flush()

                file_path = os.path.join(upload_dir, f"{doc_record.id}_{filename}")
                f.save(file_path)
                
                # Link doc to resolution via JSON
                basis = res.result_basis
                basis["ratification"]["proof_document_id"] = doc_record.id
                from sqlalchemy.orm.attributes import flag_modified
                res.result_basis = basis
                flag_modified(res, "result_basis")'''

if old_doc_pattern.search(content):
    content = old_doc_pattern.sub(new_doc_logic, content)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: Route document logic patched")
else:
    print("FAILED: Regex did not match")
