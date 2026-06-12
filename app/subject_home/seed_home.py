from app import create_app
from app.extensions import db
from app.models.home import HomeChapter

app = create_app()

chapters = [
    (1, "Observation", "Learn to observe objects and details.", "chapter1_observation.jpg"),
    (2, "Position and Direction", "Learn how objects are positioned relative to one another.", "chapter2_position.jpg"),
    (3, "Comparison", "Learn to compare size, length, height and quantity.", "chapter3_comparison.jpg"),
    (4, "Estimation", "Learn to make sensible guesses before counting or measuring.", "chapter4_estimation.jpg"),
    (5, "Measurement", "Learn to measure length, height, weight and distance.", "chapter5_measurement.jpg"),
    (6, "Pattern Recognition", "Learn to identify and predict repeating patterns.", "chapter6_patterns.jpg"),
    (7, "Spatial Reasoning", "Understand position, direction and relationships between objects.", "chapter7_spatial.jpg"),
    (8, "Logic", "Learn to use clues and evidence to make correct conclusions.", "chapter8_logic.jpg"),
    (9, "Mathematics", "Apply number concepts to solve everyday problems.", "chapter9_mathematics.png"),
    (10, "Critical Thinking", "Use observation, logic and mathematics together to solve problems.", "chapter10_critical_thinking.png"),
]

if __name__ == "__main__":
    with app.app_context():

        # clear table first
        HomeChapter.query.delete()

        # insert fresh seed data
        for number, title, objective, image in chapters:
            db.session.add(
                HomeChapter(
                    chapter_number=number,
                    title=title,
                    objective=objective,
                    image_filename=image,
                    pass_mark=100
                )
            )

        db.session.commit()

        print("HOME chapters loaded.")