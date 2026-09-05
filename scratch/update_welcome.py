import re

with open('templates/public/welcome.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add the new card to the dictionary
old_dict = '"reading":{"color":"emerald","desc":"Learn to read from scratch with the Litre Method."},'
new_dict = '"sace_cptd":{"color":"indigo","desc":"Sace CPTD Reading Activity for Teachers."},\n"reading":{"color":"emerald","desc":"Learn to read from scratch with the Litre Method."},'

content = content.replace(old_dict, new_dict)

with open('templates/public/welcome.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Added SACE CPTD card to welcome.html")
