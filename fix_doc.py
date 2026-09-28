import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

bad_doc = '''                doc_record = UipDocument(
                    organization_id=org.id,
                    resolution_id=res.id,
                    title=f"Proof: {res.title}",
                    category="MANDATE",
                    access_classification="public",
                    uploaded_by=current_user.id,
                    effective_date=datetime.utcnow().date(),
                    filename=filename,
                    size_bytes=0,
                    current_version=1
                )
                db.session.add(doc_record)
                db.session.flush()

                file_path = os.path.join(upload_dir, f"{doc_record.id}_{filename}")
                f.save(file_path)
                doc_record.size_bytes = os.path.getsize(file_path)'''

good_doc = '''                doc_record = UipDocument(
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

content = content.replace(bad_doc, good_doc)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Route document logic fixed")
