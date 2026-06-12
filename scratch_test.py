import json
from app import create_app, db
from app.utils.billing_helpers import calculate_metsoa_page1

app = create_app()
with app.app_context():
    # Assuming tenant_id = 3 and month is the one from the request, maybe "2026-06"
    res = calculate_metsoa_page1(3, "2026-06")
    print(res)
