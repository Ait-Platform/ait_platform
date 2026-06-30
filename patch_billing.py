import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the duplicate BilExtractionLog injection block
bad_injection_block = '''        try:
            from app.models.billing import BilExtractionLog
            from flask_login import current_user
            
            if current_user.is_authenticated:
                # Helper to safely parse float
                def _safe_float(val):
                    try:
                        return float(val) if val is not None else 0.0
                    except (ValueError, TypeError):
                        return 0.0
                        
                log_entry = BilExtractionLog(
                    manager_id=current_user.id,
                    property_name=data.get("property_name"),
                    address=data.get("address"),
                    metro_account_no=data.get("metro_account_no"),
                    muni_email=data.get("muni_email"),
                    has_rates=bool(data.get("has_rates")),
                    rates_amount=_safe_float(data.get("rates_amount")),
                    amount_due=_safe_float(data.get("amount_due")),
                    raw_json=data
                )
                from app.extensions import db
                db.session.add(log_entry)
                db.session.commit()
        except Exception as inner_e:
            import logging
            logging.error(f"Failed to save BilExtractionLog: {inner_e}")
'''
# We want to remove ALL occurrences of it, and then explicitly add it ONLY in parse_bill_onboarding_api where it belongs.
content = content.replace(bad_injection_block, "")

# Now let's fix google.generativeai
# We'll use a very robust approach: parsing the file and replacing the generativeai code in each function
def replace_gemini(func_text):
    # This function takes the body of a parse_bill function and updates it
    
    # 1. Imports
    func_text = re.sub(r'import google\.generativeai as genai', r'from google import genai\n        from google.genai import types', func_text)
    
    # 2. Config
    func_text = re.sub(r'genai\.configure\(api_key=api_key\)', r'client = genai.Client(api_key=api_key)', func_text)
    
    # 3. prompt_parts construction
    func_text = re.sub(r"prompt_parts\.append\(\{'mime_type': mime_type, 'data': file_bytes\}\)", 
                       r"prompt_parts.append(types.Part.from_bytes(data=file_bytes, mime_type=mime_type))", func_text)
                       
    # 4. Generate content
    # Some use: response = model.generate_content(prompt_parts)
    # Some use: response = model.generate_content([...], generation_config={...})
    
    # To be safe, we can just replace the model initialization and generate_content call
    func_text = re.sub(r"model = genai\.GenerativeModel\('gemini-1\.5-flash'\)", "", func_text)
    
    # For parse_bill_api (which builds prompt_parts and appends prompt):
    func_text = re.sub(r"response = model\.generate_content\(prompt_parts\)", 
                       r"response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt_parts)", func_text)
                       
    # For parse_readings_api (which uses a list):
    old_call_1 = '''response = model.generate_content([
            {'mime_type': mime_type, 'data': file_bytes},
            prompt
        ])'''
    new_call_1 = '''response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[types.Part.from_bytes(data=file_bytes, mime_type=mime_type), prompt]
        )'''
    func_text = func_text.replace(old_call_1, new_call_1)
    
    # For parse_bill_onboarding_api (which uses a list and generation_config):
    old_call_2 = '''response = model.generate_content([
            {'mime_type': mime_type, 'data': file_bytes},
            prompt
        ], generation_config={"response_mime_type": "application/json"})'''
    new_call_2 = '''response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[types.Part.from_bytes(data=file_bytes, mime_type=mime_type), prompt],
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )'''
    func_text = func_text.replace(old_call_2, new_call_2)
    
    return func_text

content = replace_gemini(content)

# Re-inject the BilExtractionLog into parse_bill_onboarding_api ONLY
target = '''        data = json.loads(text_response.strip())
        
        return jsonify(data)'''

injection = '''        data = json.loads(text_response.strip())
        
''' + bad_injection_block + '''
        return jsonify(data)'''

# find where parse_bill_onboarding_api starts and replace only there
start_idx = content.find('def parse_bill_onboarding_api():')
if start_idx != -1:
    end_idx = content.find('def input_readings(', start_idx)
    if end_idx == -1: end_idx = len(content)
    
    sub = content[start_idx:end_idx]
    sub = sub.replace(target, injection)
    content = content[:start_idx] + sub + content[end_idx:]

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("done")
