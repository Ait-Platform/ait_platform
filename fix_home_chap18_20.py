from app import create_app
from app.extensions import db
from app.models.home import HomeQuestion, HomeQuestionOption, HomeChapter

app = create_app()

updates = {
    18: [
        {
            "q": "If all cats have tails and Tom is a cat, what does Tom have?",
            "options": ["Tail", "Wings", "Horns"],
            "answer": "Tail"
        },
        {
            "q": "If it is raining outside, what is the most logical thing to take with you?",
            "options": ["Umbrella", "Sunglasses", "Sandals"],
            "answer": "Umbrella"
        },
        {
            "q": "If every bird has feathers, and a parrot is a bird, what does a parrot have?",
            "options": ["Feathers", "Scales", "Fur"],
            "answer": "Feathers"
        },
        {
            "q": "Which of these does not belong with the others?",
            "options": ["Apple", "Car", "Bus"],
            "answer": "Apple"
        },
        {
            "q": "If water freezes at zero degrees, what happens when it is minus five degrees?",
            "options": ["It becomes ice", "It becomes steam", "It boils"],
            "answer": "It becomes ice"
        }
    ],
    20: [
        {
            "q": "To save water while brushing your teeth, what is the best choice?",
            "options": ["Turn off the tap", "Leave the tap running", "Use a large bucket"],
            "answer": "Turn off the tap"
        },
        {
            "q": "If the library is closer than the park, which one takes less time to walk to?",
            "options": ["The library", "The park", "They take the same time"],
            "answer": "The library"
        },
        {
            "q": "If a room is too dark to read, what is the best solution?",
            "options": ["Turn on a light", "Open an umbrella", "Close the curtains"],
            "answer": "Turn on a light"
        },
        {
            "q": "To get to a far away city quickly, which is usually the best choice?",
            "options": ["Taking an airplane", "Walking slowly", "Riding a bicycle"],
            "answer": "Taking an airplane"
        },
        {
            "q": "If someone is wearing a heavy winter coat and scarf, what can you conclude?",
            "options": ["It is cold outside", "It is very hot outside", "They are going swimming"],
            "answer": "It is cold outside"
        }
    ]
}

with app.app_context():
    for cnum, qlist in updates.items():
        chap = HomeChapter.query.filter_by(chapter_number=cnum).first()
        if not chap:
            print(f"Chapter {cnum} not found!")
            continue
            
        questions_to_delete = HomeQuestion.query.filter_by(chapter_id=chap.id).all()
        for q in questions_to_delete:
            HomeQuestionOption.query.filter_by(question_id=q.id).delete()
        HomeQuestion.query.filter_by(chapter_id=chap.id).delete()
        db.session.commit()
        
        # Reset sequence in Postgres (if using Postgres, otherwise harmless in SQLite)
        db.session.execute(db.text("SELECT setval('home_questions_id_seq', COALESCE((SELECT MAX(id)+1 FROM home_questions), 1), false)"))
        db.session.execute(db.text("SELECT setval('home_question_options_id_seq', COALESCE((SELECT MAX(id)+1 FROM home_question_options), 1), false)"))
        db.session.commit()
        
        for q_data in qlist:
            q = HomeQuestion(chapter_id=chap.id, question=q_data["q"], question_type="single_select", correct_answer=q_data["answer"])
            db.session.add(q)
            db.session.flush()
            for idx, opt_text in enumerate(q_data["options"]):
                opt = HomeQuestionOption(question_id=q.id, option_text=opt_text, sort_order=idx+1)
                db.session.add(opt)
                
        db.session.commit()
        print(f"Updated Chapter {cnum} with {len(qlist)} questions.")
