with open("app/program_uip/__init__.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Add a 403 error handler to print stack trace to a file
handler = """
import traceback
@uip_bp.errorhandler(403)
def handle_403(e):
    with open("403_trace.log", "a") as log:
        log.write("\\n--- 403 FORBIDDEN ---\\n")
        log.write("Endpoint: " + str(request.endpoint) + "\\n")
        traceback.print_stack(file=log)
        log.write("Description: " + str(e.description) + "\\n")
    return "Forbidden. Stack logged.", 403
"""

if "handle_403" not in text:
    text += handler
    with open("app/program_uip/__init__.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Added 403 error handler")
