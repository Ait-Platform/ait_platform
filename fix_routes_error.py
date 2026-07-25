import re

with open('app/program_culturalfire/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Wrap vote_item in try/except
vote_item_match = re.search(r'def vote_item\(\):(.*?)(?=\n@|\Z)', content, re.DOTALL)
if vote_item_match:
    old_body = vote_item_match.group(1)
    new_body = '''
    try:''' + old_body.replace('\n', '\n    ') + '''
    except Exception as e:
        import traceback
        traceback.print_exc()
        db.session.rollback()
        return jsonify({"success": False, "message": "Server error: " + str(e)})'''
    content = content.replace(old_body, new_body)

# Wrap vote_mc in try/except (if it doesn't already have one covering everything)
# Actually, vote_mc already has a try block. Let's see:
vote_mc_match = re.search(r'def vote_mc\(\):\n\s+try:(.*?)\n\s+except Exception as e:(.*?)(?=\n@|\Z)', content, re.DOTALL)
if vote_mc_match:
    # Ensure it returns the exception string
    old_except = vote_mc_match.group(2)
    new_except = '''
        import traceback
        traceback.print_exc()
        db.session.rollback()
        return jsonify({"success": False, "message": "Server error: " + str(e)})'''
    content = content.replace(old_except, new_except)

with open('app/program_culturalfire/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Done fixing routes error handling")
