import re

def main():
    filepath = r'templates/school_home/certificate.html'
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace the width="{{ ... | default(0, true) }}%" pattern
    pattern = r'width="\{\{\s*assessment\.([a-zA-Z0-9_]+)\s*\|\s*default\(0,\s*true\)\s*\}\}%"'
    replacement = r'width="{{ assessment.\1 if assessment.\1 > 0 else 1 }}%"'
    
    content = re.sub(pattern, replacement, content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == '__main__':
    main()
