import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

upload_original = '''        upload_folder = os.path.join(current_app.root_path, "static", "uploads", "mechanic")
        os.makedirs(upload_folder, exist_ok=True)
        filename = secure_filename(file.filename)
        filename = f"{int(time.time())}_{filename}"
        file.save(os.path.join(upload_folder, filename))
        
        # Return URL for preview and the filename for saving
        file_url = url_for('static', filename=f'uploads/mechanic/{filename}')
        return jsonify({"url": file_url, "filename": filename})'''

upload_new = '''        upload_folder = os.path.join(current_app.root_path, "static", "uploads", "mechanic")
        os.makedirs(upload_folder, exist_ok=True)
        filename = secure_filename(file.filename)
        filename = f"{int(time.time())}_{filename}"
        file_path = os.path.join(upload_folder, filename)
        file.save(file_path)
        
        # AI Extraction
        ai_data = None
        try:
            from google import genai
            from google.genai import types
            from dotenv import load_dotenv
            import json
            
            dotenv_path = os.path.join(current_app.root_path, '..', '.env')
            load_dotenv(dotenv_path, override=True)
            
            api_key = os.environ.get("GEMINI_API_KEY") or current_app.config.get("GEMINI_API_KEY")
            if not api_key:
                try:
                    with open(dotenv_path, 'r', encoding='utf-8') as ef:
                        for line in ef:
                            if line.startswith('GEMINI_API_KEY='):
                                api_key = line.split('=', 1)[1].strip().strip('"').strip("'")
                                break
                except Exception:
                    pass
            
            if api_key:
                client = genai.Client(api_key=api_key)
                
                with open(file_path, "rb") as f_img:
                    file_bytes = f_img.read()
                    
                mime_type = file.mimetype
                if mime_type not in ['image/jpeg', 'image/png']:
                    mime_type = 'image/jpeg' # Fallback
                
                prompt = """
                Analyze this South African vehicle license disk. Extract the following details:
                - "vin": The 17-character VIN Number
                - "reg": The Vehicle Registration Number (License Plate)
                - "make": The Make of the vehicle (e.g., NISSAN)
                - "model": The Model or Description (e.g., Pick-up / Bakkie, or specific model if found)
                - "year": The year of the vehicle. You can often infer this from the "Date of test", "Date of liability", or the expiry date minus 1 year.
                
                Return the result strictly as a valid JSON object with the keys "vin", "reg", "make", "model", and "year".
                If a detail cannot be clearly read or found, return an empty string "" for that key. Do not include markdown formatting like `json.
                """
                
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=[types.Part.from_bytes(data=file_bytes, mime_type=mime_type), prompt],
                    config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                
                ai_data = json.loads(response.text.strip())
        except Exception as e:
            current_app.logger.error(f"Failed to extract VIN details via AI: {e}")
        
        # Return URL for preview and the filename for saving
        file_url = url_for('static', filename=f'uploads/mechanic/{filename}')
        return jsonify({"url": file_url, "filename": filename, "ai_data": ai_data})'''

content = content.replace(upload_original, upload_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated upload_disk route")
