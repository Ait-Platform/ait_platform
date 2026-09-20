with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

bad_text = """    except Exception as e:
            import logging
            logging.error(f"AUTO PATCH FAILED: {e}")
        pass"""

good_text = """    except Exception:
        pass"""

text = text.replace(bad_text, good_text)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed syntax error")
