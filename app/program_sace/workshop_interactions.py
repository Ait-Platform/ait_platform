"""Reading workshop response choices; JSON evidence only, never authority."""

FACILITATOR_QUESTIONS = (
    ('method_clear', 'Did the facilitator explain the LITRE method clearly?'),
    ('activities_clear', 'Did the facilitator demonstrate the LITRE activities clearly?'),
    ('helpful_guidance', 'Did the facilitator provide helpful guidance when you needed assistance?'),
)
EXPERIENCE_QUESTIONS = (
    ('reading_problem', 'Did the workshop help you understand the seriousness of the reading problem?'),
    ('practical_activities', 'Did the practical activities help you understand how the LITRE method works?'),
    ('oral_activities', 'Did the oral activities help you understand how sounds are blended to form words?'),
    ('able_to_use', 'Do you feel able to use the LITRE method?'),
)
CHOICES = ('Yes', 'Unsure', 'No')


def answers_valid(answers, questions):
    return (isinstance(answers, dict) and set(answers) == {key for key, _ in questions}
            and all(isinstance(value, str) and value in CHOICES for value in answers.values()))
