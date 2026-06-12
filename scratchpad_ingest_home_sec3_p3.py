import os
from app import create_app
from app.extensions import db
from app.models.home import HomeChapter, HomeQuestion, HomeQuestionOption

app = create_app()

chapters_data = [
    {
        "chapter_number": 26,
        "title": "PATTERN RECOGNITION",
        "objective": "Understand patterns and learn how repeating and growing sequences help us recognise order and predict what comes next.",
        "questions": [
            # Knowledge Check
            {"q": "What is a pattern?", "options": ["A sequence that follows a rule", "A random collection"], "answer": "A sequence that follows a rule"},
            {"q": "Which is a repeating pattern?", "options": ["Red, Blue, Red, Blue", "Red, Blue, Green"], "answer": "Red, Blue, Red, Blue"},
            {"q": "Which is a growing pattern?", "options": ["1, 2, 3, 4", "1, 1, 1, 1"], "answer": "1, 2, 3, 4"},
            {"q": "A pattern rule tells us:", "options": ["How the pattern works", "Who created it"], "answer": "How the pattern works"},
            {"q": "Patterns help us:", "options": ["Predict what comes next", "Hide information"], "answer": "Predict what comes next"},
            # Application Questions
            {"q": "What comes next? Circle, Square, Circle, Square, ___", "options": ["Circle", "Triangle"], "answer": "Circle"},
            {"q": "What comes next? 2, 4, 6, 8, ___", "options": ["10", "12"], "answer": "10"},
            {"q": "Which pattern is repeating?", "options": ["A, B, A, B", "A, B, C, D"], "answer": "A, B, A, B"},
            {"q": "Why are patterns useful?", "options": ["They help us identify order", "They make objects heavier"], "answer": "They help us identify order"},
            {"q": "Which statement is correct?", "options": ["Patterns follow rules", "Patterns are always random"], "answer": "Patterns follow rules"}
        ]
    }
]

with app.app_context():
    for data in chapters_data:
        chap = HomeChapter.query.filter_by(chapter_number=data["chapter_number"]).first()
        if not chap:
            chap = HomeChapter(chapter_number=data["chapter_number"], title=data["title"], objective=data["objective"], pass_mark=70)
            db.session.add(chap)
            db.session.commit()
            print(f"Created Chapter {chap.chapter_number}")
        else:
            chap.title = data["title"]
            chap.objective = data["objective"]
            db.session.commit()
            print(f"Updated Chapter {chap.chapter_number}")
            
        # Clear existing questions
        questions_to_delete = HomeQuestion.query.filter_by(chapter_id=chap.id).all()
        for q in questions_to_delete:
            HomeQuestionOption.query.filter_by(question_id=q.id).delete()
        HomeQuestion.query.filter_by(chapter_id=chap.id).delete()
        db.session.commit()
        
        # Reset sequence in Postgres
        db.session.execute(db.text("SELECT setval('home_questions_id_seq', COALESCE((SELECT MAX(id)+1 FROM home_questions), 1), false)"))
        db.session.execute(db.text("SELECT setval('home_question_options_id_seq', COALESCE((SELECT MAX(id)+1 FROM home_question_options), 1), false)"))
        db.session.commit()
        
        for q_data in data["questions"]:
            q = HomeQuestion(chapter_id=chap.id, question=q_data["q"], question_type="single_select", correct_answer=q_data["answer"])
            db.session.add(q)
            db.session.flush()
            
            for idx, opt_text in enumerate(q_data["options"]):
                opt = HomeQuestionOption(question_id=q.id, option_text=opt_text, sort_order=idx+1)
                db.session.add(opt)
                
        db.session.commit()
        print(f"Inserted questions for Chapter {chap.chapter_number}")

print("DONE.")
