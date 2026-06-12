import os
from app import create_app
from app.extensions import db
from app.models.home import HomeChapter, HomeQuestion, HomeQuestionOption

app = create_app()

chapters_data = [
    {
        "chapter_number": 27,
        "title": "SPATIAL REASONING",
        "objective": "Understand how objects relate to one another in space and how people use spatial awareness to navigate and solve problems.",
        "questions": [
            {"q": "Spatial reasoning helps us understand:", "options": ["How objects relate in space", "How objects change colour"], "answer": "How objects relate in space"},
            {"q": "Which activity uses spatial reasoning?", "options": ["Building a tower", "Reading a poem"], "answer": "Building a tower"},
            {"q": "Position tells us:", "options": ["Where something is", "How much it costs"], "answer": "Where something is"},
            {"q": "Which word describes position?", "options": ["Between", "Heavy"], "answer": "Between"},
            {"q": "Spatial reasoning helps us:", "options": ["Navigate", "Sleep"], "answer": "Navigate"},
            {"q": "Why is spatial reasoning useful when packing a box?", "options": ["It helps objects fit efficiently", "It changes object colours"], "answer": "It helps objects fit efficiently"},
            {"q": "Which activity requires spatial awareness?", "options": ["Reading a map", "Eating lunch"], "answer": "Reading a map"},
            {"q": "Which object is likely to fit inside a small box?", "options": ["Pencil", "Chair"], "answer": "Pencil"},
            {"q": "Why do builders use spatial reasoning?", "options": ["To understand space and structure", "To change weather conditions"], "answer": "To understand space and structure"},
            {"q": "Spatial reasoning helps us:", "options": ["Solve positioning problems", "Grow plants"], "answer": "Solve positioning problems"}
        ]
    },
    {
        "chapter_number": 28,
        "title": "LOGIC",
        "objective": "Understand how facts, clues and evidence can be used to reach sensible conclusions.",
        "questions": [
            {"q": "What is logic?", "options": ["Using reasoning and facts", "Guessing randomly"], "answer": "Using reasoning and facts"},
            {"q": "Evidence helps us:", "options": ["Reach conclusions", "Change objects"], "answer": "Reach conclusions"},
            {"q": "A conclusion is:", "options": ["A decision based on evidence", "A random guess"], "answer": "A decision based on evidence"},
            {"q": "Logic helps us:", "options": ["Solve problems", "Change colours"], "answer": "Solve problems"},
            {"q": "Which is evidence?", "options": ["Wet footprints", "A chair"], "answer": "Wet footprints"},
            {"q": "The floor is wet and a mop is nearby. What is a logical conclusion?", "options": ["Someone cleaned the floor", "The floor became wet by magic"], "answer": "Someone cleaned the floor"},
            {"q": "Which statement is logical?", "options": ["Plants need water to grow", "Plants grow faster without water"], "answer": "Plants need water to grow"},
            {"q": "Why do detectives use logic?", "options": ["To understand evidence", "To create evidence"], "answer": "To understand evidence"},
            {"q": "Logic helps us make:", "options": ["Better decisions", "Random decisions"], "answer": "Better decisions"},
            {"q": "Which process uses logic?", "options": ["Examining clues", "Ignoring clues"], "answer": "Examining clues"}
        ]
    },
    {
        "chapter_number": 29,
        "title": "MATHEMATICS",
        "objective": "Understand how mathematics is used to count, measure, calculate and solve everyday problems.",
        "questions": [
            {"q": "Mathematics involves:", "options": ["Numbers and calculations", "Painting walls"], "answer": "Numbers and calculations"},
            {"q": "Addition means:", "options": ["Combining quantities", "Removing quantities"], "answer": "Combining quantities"},
            {"q": "Subtraction means:", "options": ["Taking away", "Adding more"], "answer": "Taking away"},
            {"q": "Division means:", "options": ["Sharing equally", "Counting backwards"], "answer": "Sharing equally"},
            {"q": "Mathematics helps us:", "options": ["Solve problems", "Change weather"], "answer": "Solve problems"},
            {"q": "If you have 5 apples and receive 2 more, how many do you have?", "options": ["7", "3"], "answer": "7"},
            {"q": "If you have R20 and spend R5, how much remains?", "options": ["R15", "R25"], "answer": "R15"},
            {"q": "What is 3 groups of 4?", "options": ["12", "7"], "answer": "12"},
            {"q": "What is 10 divided by 2?", "options": ["5", "20"], "answer": "5"},
            {"q": "Why is mathematics useful?", "options": ["It helps solve practical problems", "It removes problems automatically"], "answer": "It helps solve practical problems"}
        ]
    },
    {
        "chapter_number": 30,
        "title": "CRITICAL THINKING",
        "objective": "Understand how to analyse information, evaluate choices and make informed decisions.",
        "questions": [
            {"q": "What is critical thinking?", "options": ["Analysing information before deciding", "Making random choices"], "answer": "Analysing information before deciding"},
            {"q": "Analysis means:", "options": ["Examining information carefully", "Ignoring information"], "answer": "Examining information carefully"},
            {"q": "A consequence is:", "options": ["The result of an action", "A type of object"], "answer": "The result of an action"},
            {"q": "Critical thinking helps us:", "options": ["Make informed decisions", "Avoid learning"], "answer": "Make informed decisions"},
            {"q": "Good decisions are based on:", "options": ["Information and reasoning", "Guesswork alone"], "answer": "Information and reasoning"},
            {"q": "You need shelter during rain. Which item is most useful?", "options": ["Umbrella", "Pencil"], "answer": "Umbrella"},
            {"q": "Why should we think before acting?", "options": ["To understand possible consequences", "To waste time"], "answer": "To understand possible consequences"},
            {"q": "Which person is using critical thinking?", "options": ["Someone comparing solutions before choosing", "Someone choosing without thinking"], "answer": "Someone comparing solutions before choosing"},
            {"q": "Why do leaders need critical thinking?", "options": ["To make good decisions", "To avoid responsibility"], "answer": "To make good decisions"},
            {"q": "Critical thinking helps us:", "options": ["Evaluate choices", "Ignore information"], "answer": "Evaluate choices"}
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
            
        questions_to_delete = HomeQuestion.query.filter_by(chapter_id=chap.id).all()
        for q in questions_to_delete:
            HomeQuestionOption.query.filter_by(question_id=q.id).delete()
        HomeQuestion.query.filter_by(chapter_id=chap.id).delete()
        db.session.commit()
        
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
