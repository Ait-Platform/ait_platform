import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''
        uploaded_file = client.files.upload(file=filepath)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[uploaded_file, prompt],
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        
        try:
            os.remove(filepath)
        except Exception:
            pass

        parsed_data = json.loads(response.text.strip())
        
        return jsonify({"ai_data": parsed_data})

    except Exception as e:
        print(f"Gemini AI Error: {e}")
'''

# Find the block from uploaded_file = client.files.upload to return jsonify({"error": str(e)}), 500
start_str = "        uploaded_file = client.files.upload(file=filepath)"
end_str = "        print(f\"Gemini AI Error: {e}\")"

start_idx = content.find(start_str)
end_idx = content.find(end_str, start_idx)

if start_idx != -1 and end_idx != -1:
    content = content[:start_idx] + replacement.strip('\n') + '\n' + content[end_idx:]
    with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Replaced successfully")
else:
    print("Could not find block to replace")
