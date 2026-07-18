with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

bad_block = '''        try:
            db.session.rollback()
        except:
            pass
'''
# Revert to original
text = text.replace(bad_block, '            db.session.rollback()\n')

# The rollback at line 166 was originally indented 8 spaces:
text = text.replace('''    except Exception as e:
            db.session.rollback()''', '''    except Exception as e:
        db.session.rollback()''')

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print('Reverted bad rollback logic!')
