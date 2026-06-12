import sys

file_path = r"D:\Users\yeshk\Documents\ait_platform\app\school_billing\routes.py"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

new_route = '''
@billing_bp.route("/api/parse_readings", methods=["POST"])
@login_required
def parse_readings_api():
    if 'bill_file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
        
    file = request.files['bill_file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
        
    try:
        import google.generativeai as genai
        file_bytes = file.read()
        
        mime_type = file.mimetype
        if mime_type == 'application/pdf':
            mime_type = 'application/pdf'
        elif mime_type in ['image/jpeg', 'image/png']:
            pass
        else:
            return jsonify({"error": "Unsupported file type. Please upload a PDF, JPG, or PNG."}), 400
            
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        prompt = \'\'\'
        Analyze this municipality bill and extract the specific meter readings for every meter found on the bill.
        Return the result strictly as a valid JSON object with a key "readings" containing an array of objects.
        Each object in the "readings" array should have:
        - "meter_number": The meter number (string)
        - "current_reading": The current reading value (number or string)
        - "current_date": The date of the current reading in YYYY-MM-DD format (string). If year is not given but month and day are, infer the most likely year based on the bill date.
        - "previous_reading": The previous reading value if listed (number or string)
        - "previous_date": The date of the previous reading in YYYY-MM-DD format (string)
        - "usage": The total consumption/usage amount for this meter during the billing period (number or string)
        
        If a previous_reading is missing but you have the current_reading and the usage, you MUST mathematically calculate the previous_reading (current_reading - usage = previous_reading) and include it.
        If a field cannot be determined and cannot be calculated, return an empty string for that field.
        Do not include markdown formatting like ```json.
        \'\'\'
        
        response = model.generate_content([
            {'mime_type': mime_type, 'data': file_bytes},
            prompt
        ])
        
        text_response = response.text.strip()
        if text_response.startswith('```json'):
            text_response = text_response[7:]
        if text_response.endswith('```'):
            text_response = text_response[:-3]
            
        import json
        data = json.loads(text_response.strip())
        
        return jsonify(data)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Failed to parse bill: {str(e)}"}), 500
'''

if "/api/parse_readings" not in content:
    content += new_route
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    print("Added /api/parse_readings")
else:
    print("Route already exists")
