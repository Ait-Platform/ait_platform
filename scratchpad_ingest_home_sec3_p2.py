import os
from app import create_app
from app.extensions import db
from app.models.home import HomeChapter, HomeQuestion, HomeQuestionOption

app = create_app()

chapters_data = [
    {
        "chapter_number": 24,
        "title": "ESTIMATION",
        "objective": "Understand estimation and learn how to make sensible guesses before counting, measuring or calculating.",
        "questions": [
            # Knowledge Check
            {"q": "What is estimation?", "options": ["A sensible guess", "An exact measurement"], "answer": "A sensible guess"},
            {"q": "An estimate should be:", "options": ["Reasonable", "Random"], "answer": "Reasonable"},
            {"q": "Which activity uses estimation?", "options": ["Guessing how many beans are in a jar", "Reading a name tag"], "answer": "Guessing how many beans are in a jar"},
            {"q": "Estimation is useful because it:", "options": ["Helps predict answers", "Changes the answer"], "answer": "Helps predict answers"},
            {"q": "Which statement is correct?", "options": ["Estimates are close to actual values", "Estimates are always exact"], "answer": "Estimates are close to actual values"},
            # Application Questions
            {"q": "A jar contains many sweets. What should you do before counting?", "options": ["Estimate", "Ignore it"], "answer": "Estimate"},
            {"q": "Which is an estimate?", "options": ["About 50 learners", "Exactly 50 learners after counting"], "answer": "About 50 learners"},
            {"q": "Why do builders estimate materials?", "options": ["To plan ahead", "To change colours"], "answer": "To plan ahead"},
            {"q": "Which estimate is most reasonable for a classroom?", "options": ["25 learners", "500 learners"], "answer": "25 learners"},
            {"q": "Estimation helps us:", "options": ["Make predictions", "Avoid thinking"], "answer": "Make predictions"}
        ]
    },
    {
        "chapter_number": 25,
        "title": "MEASUREMENT",
        "objective": "Understand measurement and learn how size, length, height, weight and capacity are determined.",
        "questions": [
            # Knowledge Check
            {"q": "What is measurement?", "options": ["Finding the size or amount of something", "Guessing randomly"], "answer": "Finding the size or amount of something"},
            {"q": "Length measures:", "options": ["How long something is", "How heavy something is"], "answer": "How long something is"},
            {"q": "Weight measures:", "options": ["How heavy something is", "How tall something is"], "answer": "How heavy something is"},
            {"q": "Capacity measures:", "options": ["How much a container can hold", "How many colours it has"], "answer": "How much a container can hold"},
            {"q": "Which tool is commonly used to measure length?", "options": ["Ruler", "Spoon"], "answer": "Ruler"},
            # Application Questions
            {"q": "Which object would you use a ruler to measure?", "options": ["Pencil", "Cloud"], "answer": "Pencil"},
            {"q": "Which container usually has greater capacity?", "options": ["Bucket", "Cup"], "answer": "Bucket"},
            {"q": "Which object is likely heavier?", "options": ["Brick", "Feather"], "answer": "Brick"},
            {"q": "Why is measurement important?", "options": ["It helps us compare accurately", "It changes object sizes"], "answer": "It helps us compare accurately"},
            {"q": "Which property can be measured?", "options": ["Height", "Favourite colour"], "answer": "Height"}
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
