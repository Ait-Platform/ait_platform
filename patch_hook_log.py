with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('except Exception:', 'except Exception as e:\n            import logging\n            logging.error(f"AUTO PATCH FAILED: {e}")')

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added error logging to hook")
