import sys
import re

file_path = r"D:\Users\yeshk\Documents\ait_platform\app\school_billing\routes.py"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

old_prompt = '''        prompt = \'\'\'
        Analyze this municipality bill and extract the following information.
        Return the result strictly as a valid JSON object with the following keys:
        - "property_name": The name of the property or owner (string)
        - "address": The full address of the property (string)
        - "metro_account_no": The municipal account number (string)
        - "water_meters": An array of water meter numbers found on the bill (array of strings)
        - "electricity_meters": An array of electricity meter numbers found on the bill (array of strings)
        If a field is not found, return an empty string or empty array for that key. Do not include markdown formatting like ```json.
        \'\'\''''

new_prompt = '''        prompt = \'\'\'
        Analyze this municipality bill and extract the following information.
        Return the result strictly as a valid JSON object with the following keys:
        - "property_name": The name of the property or owner (string)
        - "address": The full address of the property (string)
        - "metro_account_no": The municipal account number (string)
        - "water_meters": An array of water meter numbers found on the bill (array of strings)
        - "electricity_meters": An array of electricity meter numbers found on the bill (array of strings)
        - "has_rates": A boolean (true or false), true if the bill includes a property rates charge (sometimes called property tax or assessment rates)
        - "rates_amount": The monetary amount charged for property rates, excluding currency symbols (string or number)
        If a field is not found, return an empty string or empty array for that key. Do not include markdown formatting like ```json.
        \'\'\''''

content = content.replace(old_prompt, new_prompt)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)
print("Routes updated successfully")
