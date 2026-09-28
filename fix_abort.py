import re

filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# I will change process_import_batch to raise Exception instead of InvalidRegisterOption for missing references,
# so it gets caught by the generic 'except Exception as e:' which adds it to the exceptions list safely.
content = content.replace('raise InvalidRegisterOption("reference", "Missing municipal reference", [])', 'raise Exception("Missing municipal reference column")')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

filepath_routes = 'app/program_uip/completion_routes.py'
with open(filepath_routes, 'r', encoding='utf-8') as f:
    content_routes = f.read()

# Remove the InvalidRegisterOption catch in routes so it doesn't abort
search_abort = '''        except InvalidRegisterOption as error:
            value = error.value
            shown = ascii(value[:80]) + ("... (truncated)" if len(value) > 80 else "") if isinstance(value, str) else ascii(value)
            allowed = ", ".join(ascii(option) for option in error.allowed)
            from flask import abort
            abort(400, description=f"CSV row {idx}: column '{error.column}' has invalid value {shown}. Accepted values: {allowed} (case-sensitive).")'''

content_routes = content_routes.replace(search_abort, "")

with open(filepath_routes, 'w', encoding='utf-8') as f:
    f.write(content_routes)
