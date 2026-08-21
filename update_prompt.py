import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

prompt_original = '''                prompt = """
                Analyze this South African vehicle license disk. Extract the following details:
                - "vin": The 17-character VIN Number
                - "reg": The Vehicle Registration Number (License Plate)
                - "make": The Make of the vehicle (e.g., NISSAN)
                - "model": The Model or Description (e.g., Pick-up / Bakkie, or specific model if found)
                - "year": The year of the vehicle. You can often infer this from the "Date of test", "Date of liability", or the expiry date minus 1 year.
                
                Return the result strictly as a valid JSON object with the keys "vin", "reg", "make", "model", and "year".
                If a detail cannot be clearly read or found, return an empty string "" for that key. Do not include markdown formatting like `json.
                """'''

prompt_new = '''                prompt = """
                Analyze this South African vehicle license disk. Extract the following details:
                - "vin": The 17-character VIN Number
                - "reg": The Vehicle Registration Number (License Plate)
                - "make": The Make of the vehicle (e.g., NISSAN)
                - "model": The Model or Description (e.g., Pick-up / Bakkie, or specific model if found)
                - "year": The year of the vehicle. You can often infer this from the "Date of test", "Date of liability", or the expiry date minus 1 year.
                - "engine_no": The Engine Number (Enjinnr.)
                - "gvm": The GVM / BVM value
                - "tare": The Tare / Tarra value
                - "disk_license_no": The printed License number (Lisensienr.) usually at the top or near the VIN.
                
                Return the result strictly as a valid JSON object with the exact keys: "vin", "reg", "make", "model", "year", "engine_no", "gvm", "tare", "disk_license_no".
                If a detail cannot be clearly read or found, return an empty string "" for that key. Do not include markdown formatting like `json.
                """'''

content = content.replace(prompt_original, prompt_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated upload_disk prompt")
