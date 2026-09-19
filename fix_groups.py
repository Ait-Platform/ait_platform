with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('group_level="EXECUTIVE"', 'group_level="CORE_EXCO"')
text = text.replace('"EXECUTIVE"', '"CORE_EXCO"')
text = text.replace('"SUBCOMMITTEE"', '"SECOND_GROUP"')

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed group_level in routes.py")
