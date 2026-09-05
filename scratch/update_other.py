import os

paths = [
    'templates/program_sace/interactive_workshop.html',
    'templates/program_sace/post_ws_survey.html',
    'templates/program_sace/simulator.html',
    'templates/program_sace/compliance/annexure_b.html',
    'templates/program_cptd/reading_timetable.html',
    'templates/program_cptd/modules/reading_1.html',
    'templates/program_cptd/modules/reading_palm.html'
]

old_terms = [
    'Litre Reading',
    'LITRE Reading',
    'LITRE Blending Machine',
    'LITRE blending-machine',
    ' blending-machine',
    ' blending machine'
]

for p in paths:
    if os.path.exists(p):
        with open(p, 'r', encoding='utf-8') as f:
            text = f.read()
        
        # for these, we will replace the whole phrase if it's describing the activity.
        # But if it's a specific question, it might break. 
        # Actually I will just replace the strict matches.
        text = text.replace('LITRE Reading Programme', 'I Learn to Read English Using the LITRE Method')
        text = text.replace('Litre Reading Programme', 'I Learn to Read English Using the LITRE Method')
        text = text.replace('LITRE Reading Workshop', 'I Learn to Read English Using the LITRE Method')
        
        with open(p, 'w', encoding='utf-8') as f:
            f.write(text)
