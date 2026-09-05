import re

def replace_in_file(path, old_texts, new_text):
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    for old in old_texts:
        text = text.replace(old, new_text)
        
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)

replace_in_file('templates/program_sace/reading_hub.html', 
    ['LITRE Reading Programme Validation Flow', 'the LITRE Blending Machine and associated methodology'], 
    'the I Learn to Read English Using the LITRE Method')
    
replace_in_file('templates/program_sace/presentation_ppp.html',
    ['Litre Reading Presentation'],
    'I Learn to Read English Using the LITRE Method Presentation')

replace_in_file('templates/program_sace/post_test/results.html',
    ['LITRE Reading Workshop'],
    'I Learn to Read English Using the LITRE Method')

replace_in_file('templates/program_cptd/evaluation.html',
    ['LITRE Reading Programme'],
    'I Learn to Read English Using the LITRE Method')
