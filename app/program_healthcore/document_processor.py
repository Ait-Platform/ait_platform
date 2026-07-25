import threading
from app.extensions import db
from app.models.healthcore import HcDocument, HcDocumentExtraction
from app.program_healthcore.ai_extractor import extract_health_document
from flask import current_app
import json

def process_documents_async(app, user_id):
    """
    Background thread to process uploaded documents with Gemini.
    """
    with app.app_context():
        pending_docs = HcDocument.query.filter_by(user_id=user_id, status='uploaded').all()
        for doc in pending_docs:
            doc.status = 'processing'
            db.session.commit()
            
            try:
                # Call Gemini extractor
                result = extract_health_document(doc.file_url) # assuming file_url is the absolute local path for now, or relative path we can resolve
                
                # Resolve full path
                import os
                full_path = os.path.join(app.root_path, 'static', doc.file_url)
                result = extract_health_document(full_path)
                
                if "error" in result:
                    doc.status = 'error'
                    doc.extracted_text = result.get("error")
                else:
                    # Save to HcDocumentExtraction
                    extraction = HcDocumentExtraction(
                        document_id=doc.id,
                        extracted_json=json.dumps(result),
                        document_type=result.get("document_type", "unknown")
                    )
                    db.session.add(extraction)
                    doc.status = 'review_ready'
                    
            except Exception as e:
                doc.status = 'error'
                doc.extracted_text = str(e)
                
            db.session.commit()
