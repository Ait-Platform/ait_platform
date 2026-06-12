import os
from app import create_app
from app.extensions import db
from app.models.home import HomeChapter, HomeQuestion, HomeQuestionOption

app = create_app()

chapters_data = [
    {
        "chapter_number": 21,
        "title": "OBSERVATION",
        "objective": "Understand what observation is and how objects can be observed using their properties.",
        "questions": [
            # Knowledge Check
            {"q": "What is observation?", "options": ["Carefully noticing information", "Running quickly"], "answer": "Carefully noticing information"},
            {"q": "What is an object?", "options": ["Anything that can be observed", "A type of colour"], "answer": "Anything that can be observed"},
            {"q": "Which is an object?", "options": ["Chair", "Blue"], "answer": "Chair"},
            {"q": "Which is a property?", "options": ["Shape", "Table"], "answer": "Shape"},
            {"q": "Which property describes an apple?", "options": ["Round", "Teacher"], "answer": "Round"},
            # Application Questions
            {"q": "Why do we observe objects?", "options": ["To gather information", "To make them bigger"], "answer": "To gather information"},
            {"q": "Which property can be observed?", "options": ["Colour", "Weather forecast"], "answer": "Colour"},
            {"q": "Which property describes a pencil?", "options": ["Long", "Classroom"], "answer": "Long"},
            {"q": "Observation helps us:", "options": ["Compare objects", "Change objects"], "answer": "Compare objects"},
            {"q": "Which statement is correct?", "options": ["Objects have properties", "Properties have objects"], "answer": "Objects have properties"}
        ]
    },
    {
        "chapter_number": 22,
        "title": "POSITION AND DIRECTION",
        "objective": "Understand how objects are positioned relative to one another and how direction words are used.",
        "questions": [
            # Knowledge Check
            {"q": "Position tells us:", "options": ["Where something is", "How much it costs"], "answer": "Where something is"},
            {"q": "Which is a position word?", "options": ["Behind", "Heavy"], "answer": "Behind"},
            {"q": "Which is a direction word?", "options": ["Left", "Tall"], "answer": "Left"},
            {"q": "Direction helps us:", "options": ["Move from one place to another", "Change colour"], "answer": "Move from one place to another"},
            {"q": "Which word describes location?", "options": ["Between", "Weight"], "answer": "Between"},
            # Application Questions
            {"q": "If a book is on a table, where is the table?", "options": ["Below the book", "Above the book"], "answer": "Below the book"},
            {"q": "Which direction is opposite to left?", "options": ["Right", "Forward"], "answer": "Right"},
            {"q": "Which direction is opposite to forward?", "options": ["Backward", "Right"], "answer": "Backward"},
            {"q": "Which word describes an object in the middle of two objects?", "options": ["Between", "Above"], "answer": "Between"},
            {"q": "Why do we use direction words?", "options": ["To help people find places", "To describe colours"], "answer": "To help people find places"}
        ]
    },
    {
        "chapter_number": 23,
        "title": "COMPARISON",
        "objective": "Understand how objects can be compared using their properties.",
        "questions": [
            # Knowledge Check
            {"q": "What is an object?", "options": ["Something that can be observed", "A type of colour"], "answer": "Something that can be observed"},
            {"q": "What is a property?", "options": ["A characteristic of an object", "A type of animal"], "answer": "A characteristic of an object"},
            {"q": "Which is a property of a book?", "options": ["Colour", "Blue"], "answer": "Colour"},
            {"q": "Comparison helps us:", "options": ["Identify similarities and differences", "Count money"], "answer": "Identify similarities and differences"},
            {"q": "Which pair has the same shape?", "options": ["Ball and orange", "Pencil and ball"], "answer": "Ball and orange"},
            # Application Questions
            {"q": "Compare a spoon and a fork. Which property is similar?", "options": ["Both are used for eating", "Both are books"], "answer": "Both are used for eating"},
            {"q": "Compare a chair and a table. Which statement is true?", "options": ["They are both furniture", "They have exactly the same purpose"], "answer": "They are both furniture"},
            {"q": "Which property can be used to compare two bottles?", "options": ["Size", "Weather"], "answer": "Size"},
            {"q": "Why do we compare objects?", "options": ["To understand similarities and differences", "To change their colour"], "answer": "To understand similarities and differences"},
            {"q": "Which comparison is correct?", "options": ["A book is usually larger than a bookmark", "A bookmark is usually larger than a book"], "answer": "A book is usually larger than a bookmark"}
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
