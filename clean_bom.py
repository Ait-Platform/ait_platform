with open("app/program_uip/committee_routes.py", "rb") as f:
    content = f.read()

# Replace any BOM or weird characters
content = content.replace(b"\xef\xbf\xbd", b"")
content = content.replace(b"\xef\xbb\xbf", b"")

with open("app/program_uip/committee_routes.py", "wb") as f:
    f.write(content)
print("Stripped bad characters")
