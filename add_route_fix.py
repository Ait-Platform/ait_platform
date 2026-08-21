import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add the route near upload_disk
route_code = '''
@mechanic_bp.route("/upload_business_card", methods=["POST"])
@login_required
def upload_business_card():
    import os
    import json
    from werkzeug.utils import secure_filename
    import google.genai as genai
    from google.genai import types
    from flask import current_app, request, jsonify

    image_file = request.files.get("business_card_image")
    if not image_file or not image_file.filename:
        return jsonify({"error": "No image uploaded"}), 400

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return jsonify({"error": "Gemini AI API key not configured on server"}), 500

    try:
        # Save temp file
        temp_dir = os.path.join(current_app.root_path, "static", "uploads", "temp")
        os.makedirs(temp_dir, exist_ok=True)
        filename = secure_filename(image_file.filename)
        filepath = os.path.join(temp_dir, filename)
        image_file.save(filepath)

        client = genai.Client(api_key=api_key)
        
        prompt = """
        Analyze this business card, letterhead, or storefront sign. Extract the following details for the business:
        - "business_name": The Name of the business.
        - "address": The physical address of the business.
        - "phone": The primary contact phone number.
        - "email": The primary contact email address.
        
        Return the result strictly as a valid JSON object with the exact keys: "business_name", "address", "phone", "email".
        If a detail cannot be clearly read or found, return an empty string "" for that key. Do not include markdown formatting like `json.
        """

        uploaded_file = client.files.upload(file=filepath)
        response = client.models.generate_content(
            model='gemini-2.5-pro',
            contents=[uploaded_file, prompt]
        )
        
        try:
            os.remove(filepath)
        except Exception:
            pass

        raw_text = response.text.strip()
        if raw_text.startswith("`json"):
            raw_text = raw_text[7:]
        if raw_text.startswith("`"):
            raw_text = raw_text[3:]
        if raw_text.endswith("`"):
            raw_text = raw_text[:-3]

        parsed_data = json.loads(raw_text.strip())
        
        return jsonify({"ai_data": parsed_data})

    except Exception as e:
        print(f"Gemini AI Error: {e}")
        return jsonify({"error": str(e)}), 500
'''

# Find def upload_disk and insert before it
content = content.replace('@mechanic_bp.route("/mechanic/api/upload_disk", methods=["POST"])', route_code + '\n@mechanic_bp.route("/mechanic/api/upload_disk", methods=["POST"])')

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Added upload_business_card route correctly")
