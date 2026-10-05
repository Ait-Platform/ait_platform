"""Reviewed HOME instruments; no Reading content or response dependencies."""
FACILITATOR_QUESTIONS = (
    ('approach_clear', 'Did the facilitator explain the Hands-On Math Education approach clearly?'),
    ('practical_guidance', 'Did the facilitator guide the practical activities effectively?'),
    ('classroom_use', 'Did the facilitator help participants understand how the practical activities can be used in the classroom?'),
)
PARTICIPANT_QUESTIONS = (
    ('approach_understood', 'Did the practical activities help you understand the Hands-On Math Education approach?'),
    ('both_roles', 'Did working as both Teacher and Learner help you understand how to use the activities?'),
    ('able_to_use', 'Do you feel able to use the HOME practical activities with learners?'),
    ('classroom_useful', 'Was the workshop useful for your classroom practice?'),
)
SURVEY_QUESTION = 'Will you use Hands-On Math Education (HOME) when you return to your school/classroom?'
CHOICES = ('Yes', 'Unsure', 'No')


def answers_valid(answers, questions):
    return (isinstance(answers, dict) and set(answers) == {key for key, _ in questions}
        and all(type(value) is str and value in CHOICES for value in answers.values()))
