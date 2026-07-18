with open('templates/admin/modules_control.html', 'r', encoding='utf-8') as f:
    content = f.read()
if 'type="submit"' in content:
    print('Save button exists')
else:
    print('No save button found')
