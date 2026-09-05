import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Fix secure_view logging order
old_secure = '''
    # Log that the user viewed this document
    interaction = SaceWorkshopInteraction(
        user_id=current_user.id,
        activity_slug=f"viewed_{doc_type}",
        response_data="Document opened in secure viewer"
    )
    db.session.add(interaction)
    db.session.commit()
    
    # Retrieve the document URL
    doc = SaceDocument.query.filter_by(document_type=doc_type).first()
    if not doc:
        flash("Document not found or not uploaded yet.", "error")
        return redirect(url_for('sace_bp.reading_hub'))
'''

new_secure = '''
    # Retrieve the document URL first
    doc = SaceDocument.query.filter_by(document_type=doc_type).first()
    if not doc:
        flash("Document not found or not uploaded yet.", "error")
        return redirect(url_for('sace_bp.reading_hub'))

    # Log that the user viewed this document
    interaction = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug=f"viewed_{doc_type}").first()
    if not interaction:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug=f"viewed_{doc_type}",
            response_data="Document opened in secure viewer"
        )
        db.session.add(interaction)
        db.session.commit()
'''

text = text.replace(old_secure, new_secure)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
