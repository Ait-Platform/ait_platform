with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

bad_line = 'db.session.commit()            flash("Members successfully officially logged into the Founding Resolution!", "success")'
good_line = 'db.session.commit()\n            flash("Members successfully officially logged into the Founding Resolution!", "success")'

if bad_line in text:
    text = text.replace(bad_line, good_line)
    with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Fixed smushed line syntax error")
else:
    print("Could not find the smushed line")
