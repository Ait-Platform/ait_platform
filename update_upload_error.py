import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Currently:
#         except Exception as e:
#             current_app.logger.error(f"Failed to extract VIN details via AI: {e}")
#         
#         # Return URL for preview and the filename for saving
#         file_url = url_for('static', filename=f'uploads/mechanic/{filename}')
#         return jsonify({"url": file_url, "filename": filename, "ai_data": ai_data})

regex = r'(\s*except Exception as e:\s*current_app\.logger\.error\(.*?\{e\}\"\)\s*)(# Return URL.*?return jsonify\(\{"url": file_url, "filename": filename, "ai_data": ai_data\}\))'

def replacer(match):
    new_except = match.group(1) + "    ai_error = str(e)\n"
    new_return = match.group(2).replace('"ai_data": ai_data}', '"ai_data": ai_data, "error": ai_error if \'ai_error\' in locals() else None}')
    return new_except + "        " + new_return

content = re.sub(regex, replacer, content, flags=re.DOTALL)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
