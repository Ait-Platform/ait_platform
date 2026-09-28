import re

filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''        except InvalidRegisterOption as error:
            value = error.value
            shown = ascii(value[:80]) + ("... (truncated)" if len(value) > 80 else "") if isinstance(value, str) else ascii(value)
            allowed = ", ".join(ascii(option) for option in error.allowed)
            from flask import abort
            abort(400, description=f"CSV row {idx}: column '{error.column}' has invalid value {shown}. Accepted values: {allowed} (case-sensitive).")'''

replace_code = '''        except InvalidRegisterOption as error:
            value = error.value
            shown = ascii(value[:80]) + ("... (truncated)" if len(value) > 80 else "") if isinstance(value, str) else ascii(value)
            allowed = ", ".join(ascii(option) for option in error.allowed)
            db.session.add(UipRegisterImportException(
                import_id=batch.id,
                row_number=idx,
                source_reference=row.get("reference") or row.get("member_reference") or "unknown",
                reason=f"Column '{error.column}' has invalid value {shown}. Accepted values: {allowed} (case-sensitive).",
                incoming_data=row
            ))
            summary["exceptions"] += 1'''

content = content.replace(search_code, replace_code)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated process_import_batch to log InvalidRegisterOption as exceptions instead of aborting")
