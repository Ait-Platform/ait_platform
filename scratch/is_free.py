import sys
with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

start = text.find('if is_free:')
end = text.find('def finalize', start)
with open('scratch/is_free_output.txt', 'w', encoding='utf-8') as out:
    out.write(text[start:end])
