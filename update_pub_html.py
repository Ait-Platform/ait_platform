import re

filepath = 'templates/program_uip/dashboards/public_mandates.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

bad_lookup = "{% set proof_doc = org.documents.filter_by(id=rat.proof_document_id).first() if rat and rat.proof_document_id else None %}"
good_lookup = "{% set proof_doc = proof_docs.get(res.id) %}"

content = content.replace(bad_lookup, good_lookup)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("public_mandates.html updated again")
