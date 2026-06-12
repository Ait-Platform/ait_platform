import sys

file_path = r"D:\Users\yeshk\Documents\ait_platform\app\school_billing\routes.py"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

import re

new_route = """@billing_bp.route("/api/parse_bill", methods=["POST"])
@login_required
def parse_bill_api():
    if 'bill_file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
        
    file = request.files['bill_file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
        
    # TODO: Integrate Google Gemini Vision API here using GEMINI_API_KEY from .env
    # 1. Read file bytes
    # 2. Send to Gemini with prompt: "Extract property_name, address, metro_account_no, and meters array..."
    # 3. Parse JSON response
    
    # Placeholder mocked response
    import time
    time.sleep(2) # simulate AI processing time
    
    return jsonify({
        "property_name": "Extracted Property",
        "address": "123 Extracted Street",
        "metro_account_no": "METRO-999-000",
        "message": "AI Integration required to parse real document. This is mock data."
    })

"""

content = content.replace('@billing_bp.route("/billing/setup", methods=["GET"])', new_route + '@billing_bp.route("/billing/setup", methods=["GET"])')

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)
print("Updated successfully")
