import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Append return_url to client_soa link
client_soa_original = '''<a href="{{ url_for('mechanic_bp.client_soa', client_id=job_card.vehicle.client.id) }}"'''
client_soa_new = '''<a href="{{ url_for('mechanic_bp.client_soa', client_id=job_card.vehicle.client.id, return_url=url_for('mechanic_bp.job_card_detail', id=job_card.id)) }}"'''

content = content.replace(client_soa_original, client_soa_new)

# 2. Remove the old View Invoice button that points to generate_invoice (which we deleted)
# AND remove the old approve button to replace it with the new POP capture one.
# Let's replace the whole action block for Billed and Quote statuses.

# Let's first view what we are dealing with using regex or slicing.
