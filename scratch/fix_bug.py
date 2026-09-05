with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

bad_str = "re.split(r'[,;\n\\s]+', emails_raw)"
good_str = "re.split(r'[,;\\\\s\\\\n]+', emails_raw)"

# The exact text in the file right now is:
# re.split(r'[,;\n\s]+', emails_raw)

if "re.split(r'[,;\n\s]+', emails_raw)" in text:
    text = text.replace("re.split(r'[,;\n\s]+', emails_raw)", "re.split(r'[,;\\s\\n]+', emails_raw)")
    with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed!")
else:
    print("Could not find exact broken string, trying fallback.")
    text = text.replace("re.split(r'[,;\n\\s]+', emails_raw)", "re.split(r'[,;\\\\s\\\\n]+', emails_raw)")
    with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fallback complete.")
