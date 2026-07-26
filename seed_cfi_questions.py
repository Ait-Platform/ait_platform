from app import create_app
from app.extensions import db
from app.models.culturalfire import CfiPageantQuestion

app = create_app()

questions = [
    "What is your biggest fear and how do you plan to overcome it?",
    "If you could change one thing in the world, what would it be?",
    "Who is your biggest role model and why?",
    "How do you define success?",
    "What does true beauty mean to you?",
    "What is the most important lesson you have learned in life?",
    "If you were given a chance to be someone else for a day, who would you be?",
    "What is the biggest challenge facing youth today?",
    "How can we promote better mental health awareness?",
    "What is your favorite book and why?",
    "If you had to choose between wealth and wisdom, which would you pick?",
    "What role does social media play in shaping society?",
    "How do you handle failure and setbacks?",
    "What makes you unique?",
    "What is the best piece of advice you have ever received?",
    "How can women empower each other in today's world?",
    "What cause are you most passionate about?",
    "Where do you see yourself in five years?",
    "How do you balance your personal and professional life?",
    "If you could send a message to your younger self, what would it say?"
]

with app.app_context():
    # Only add if the table is empty
    if CfiPageantQuestion.query.count() == 0:
        for text in questions:
            q = CfiPageantQuestion(question_text=text)
            db.session.add(q)
        db.session.commit()
        print("Successfully added 20 questions to the database.")
    else:
        print(f"Questions already exist in the database (count: {CfiPageantQuestion.query.count()}).")
