# app/home/chapters.py

CHAPTERS = {
    1: {
        "title": "Observation",
        "objective": "Observe and describe visible features in an image.",
        "image": "roads.jpg",
        "questions": [
            {
                "id": 1,
                "question": "How many roads do you see?",
                "type": "radio",
                "options": ["1", "2", "3", "4"],
                "answer": "2",
                "points": 10
            },
            {
                "id": 2,
                "question": "Which road appears straight?",
                "type": "radio",
                "options": ["Road A", "Road B"],
                "answer": "Road A",
                "points": 10
            },
            {
                "id": 3,
                "question": "Which road appears curved?",
                "type": "radio",
                "options": ["Road A", "Road B"],
                "answer": "Road B",
                "points": 10
            }
        ]
    }
}