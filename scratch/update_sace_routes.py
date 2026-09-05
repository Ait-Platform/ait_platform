import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Add the complete route
complete_route = '''
@sace_bp.route("/sace/reading/presentation/complete")
@login_required
def presentation_complete():
    from app.models.sace import SaceWorkshopInteraction
    
    # Log that the user viewed the PPP
    interaction = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="viewed_ppp").first()
    if not interaction:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="viewed_ppp",
            response_data="Linear presentation completed"
        )
        db.session.add(interaction)
        db.session.commit()
        
    flash("Linear Presentation completed successfully.", "success")
    return redirect(url_for('sace_bp.reading_hub'))
'''

if 'def presentation_complete()' not in text:
    text = text.replace('def presentation():\n    return render_template("program_sace/presentation_ppp.html")', 
                        'def presentation():\n    return render_template("program_sace/presentation_ppp.html")\n' + complete_route)

# Now, fix secure_view to NOT require a document in the database (for testing mode / missing docs)
# Instead of returning to hub, let's just create a dummy URL if missing, and log the interaction anyway!
old_secure = '''    doc = SaceDocument.query.filter_by(document_type=doc_type).first()
    if not doc:
        flash("Document not found or not uploaded yet.", "error")
        return redirect(url_for('sace_bp.reading_hub'))'''

new_secure = '''    doc = SaceDocument.query.filter_by(document_type=doc_type).first()
    # TESTING FIX: If doc is missing, we still want to log the interaction so the user can proceed in the map!
    doc_url = doc.document_url if doc else url_for('static', filename='images/dummy_document.pdf')'''

text = text.replace(old_secure, new_secure)

# Update the render_template in secure_view to use doc_url instead of doc
# Wait, let's check what secure_view returns.
