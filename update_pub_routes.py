import re

filepath = 'app/program_uip/routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

bad_route = '''    adopted_resolutions = UipResolution.query.filter_by(
        organization_id=org.id, 
        status='ADOPTED'
    ).order_by(
        UipResolution.decision_date.desc().nullslast(), 
        UipResolution.created_at.desc(), UipResolution.id.desc()
    ).all()
    
    return render_template("program_uip/dashboards/public_mandates.html", org=org, resolutions=adopted_resolutions)'''

good_route = '''    adopted_resolutions = UipResolution.query.filter_by(
        organization_id=org.id, 
        status='ADOPTED'
    ).order_by(
        UipResolution.decision_date.desc().nullslast(), 
        UipResolution.created_at.desc(), UipResolution.id.desc()
    ).all()
    
    from app.models.uip import UipDocument
    proof_docs = {}
    for res in adopted_resolutions:
        if res.result_basis and res.result_basis.get("ratification") and res.result_basis["ratification"].get("proof_document_id"):
            doc_id = res.result_basis["ratification"]["proof_document_id"]
            doc = UipDocument.query.get(doc_id)
            if doc:
                proof_docs[res.id] = doc
                
    return render_template("program_uip/dashboards/public_mandates.html", org=org, resolutions=adopted_resolutions, proof_docs=proof_docs)'''

content = content.replace(bad_route, good_route)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("routes.py updated")
