import sys

file_path = r"D:\Users\yeshk\Documents\ait_platform\app\school_billing\routes.py"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

import re

old_route = re.compile(r'@billing_bp\.route\("/api/parse_bill", methods=\["POST"\]\).*?def parse_bill_api\(\):.*?return jsonify\(\{.*?\}\)\n', re.DOTALL)

new_route = """@billing_bp.route("/api/parse_bill", methods=["POST"])
@login_required
def parse_bill_api():
    if 'bill_file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
        
    file = request.files['bill_file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
        
    try:
        import google.generativeai as genai
        import os
        import json
        
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            return jsonify({"error": "GEMINI_API_KEY is not configured"}), 500
            
        genai.configure(api_key=api_key)
        
        file_bytes = file.read()
        mime_type = file.content_type
        
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        prompt = '''
        Analyze this municipality bill and extract the following information.
        Return the result strictly as a valid JSON object with the following keys:
        - "property_name": The name of the property or owner (string)
        - "address": The full address of the property (string)
        - "metro_account_no": The municipal account number (string)
        If a field is not found, return an empty string for that key. Do not include markdown formatting like ```json.
        '''
        
        response = model.generate_content([
            {'mime_type': mime_type, 'data': file_bytes},
            prompt
        ])
        
        text_response = response.text.strip()
        if text_response.startswith('```json'):
            text_response = text_response[7:]
        if text_response.endswith('```'):
            text_response = text_response[:-3]
            
        data = json.loads(text_response.strip())
        
        return jsonify(data)
        
    except Exception as e:
        return jsonify({"error": f"Failed to parse bill: {str(e)}"}), 500
"""

content = old_route.sub(new_route, content)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)
print("Updated successfully")
