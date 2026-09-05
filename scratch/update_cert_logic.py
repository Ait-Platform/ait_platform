import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

submit_logic_old = '''    answers = {
        'q1': request.form.get('q1'),
        'q2': request.form.get('q2'),
        'q3': request.form.get('q3'),
        'q4': request.form.get('q4')
    }
    
    interaction = SaceWorkshopInteraction('''

submit_logic_new = '''    q1 = request.form.get('q1')
    q2 = request.form.get('q2')
    q3 = request.form.get('q3')
    q4 = request.form.get('q4')
    
    score = 0
    if q1 == 'B': score += 25
    if q2 == 'B': score += 25
    if q3 == 'C': score += 25
    if q4 == 'A': score += 25
    
    competencies = []
    for key in ['comp_objective', 'comp_sequence', 'comp_demo', 'comp_participation', 'comp_guidance', 'comp_reading', 'comp_assessment', 'comp_reflection']:
        val = request.form.get(key)
        if val:
            competencies.append(val)
            
    answers = {
        'q1': q1,
        'q2': q2,
        'q3': q3,
        'q4': q4,
        'score': score,
        'competencies': competencies
    }
    
    interaction = SaceWorkshopInteraction('''

if submit_logic_old in text:
    text = text.replace(submit_logic_old, submit_logic_new)
else:
    print("Failed to replace submit_post_test logic")


# Also update email_certificate to fetch interaction and pass it
email_old = '''    try:
        # Generate the standard PDF
        pdf_bytes = _generate_sace_certificate_pdf(
            certificate_id=cert_id,
            learner_name=current_user.name,
            completed_at=completed_at,
            user_id=current_user.id
        )'''

email_new = '''    from app.models.sace import SaceWorkshopInteraction
    import json
    interaction = SaceWorkshopInteraction.query.filter_by(
        user_id=current_user.id,
        activity_slug='workshop_post_test'
    ).order_by(SaceWorkshopInteraction.timestamp.desc()).first()
    
    answers = {}
    if interaction:
        answers = json.loads(interaction.response_data)

    try:
        # Generate the standard PDF
        pdf_bytes = _generate_sace_certificate_pdf(
            certificate_id=cert_id,
            learner_name=current_user.name,
            completed_at=completed_at,
            user_id=current_user.id,
            answers=answers
        )'''
        
if email_old in text:
    text = text.replace(email_old, email_new)
else:
    print("Failed to replace email_certificate logic")


# Also update the definition of _generate_sace_certificate_pdf
gen_old = 'def _generate_sace_certificate_pdf(certificate_id, learner_name, completed_at, user_id=None):'
gen_new = 'def _generate_sace_certificate_pdf(certificate_id, learner_name, completed_at, user_id=None, answers=None):'
text = text.replace(gen_old, gen_new)


# Also pass answers to render_template
rt_old = '''            certificate_id=certificate_id,
            logo_path=logo_data_uri,
            seal_path=seal_data_uri,'''
rt_new = '''            certificate_id=certificate_id,
            logo_path=logo_data_uri,
            seal_path=seal_data_uri,
            answers=answers,'''
if rt_old in text:
    text = text.replace(rt_old, rt_new)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

