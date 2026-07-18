with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

bad_text = '''        except Exception as e:
        db.session.rollback()'''

good_text = '''        except Exception as e:
            try:
                db.session.rollback()
            except:
                pass'''

text = text.replace(bad_text, good_text)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print('Fixed line 1082!')
