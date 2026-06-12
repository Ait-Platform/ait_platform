import os
import json
import time
from google import genai
from app import create_app
from app.extensions import db
from app.models.adv_math import AdvMathQuestion

def test_extract():
    import dotenv
    dotenv.load_dotenv(r"D:\Users\yeshk\Documents\ait_platform\.env")
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

    app = create_app()

    q_pdf_path = r"D:\Users\yeshk\Documents\ait_platform\app\data\dbe_papers\Mathematics P1 May-June 2025 Eng.pdf"
    m_pdf_path = r"D:\Users\yeshk\Documents\ait_platform\app\data\dbe_papers\Mathematics P1 May-June 2025 Approved Marking Guideline.pdf"
    
    print("Uploading Question PDF...")
    q_file = client.files.upload(file=q_pdf_path)
    print("Uploading Memo PDF...")
    m_file = client.files.upload(file=m_pdf_path)
    
    # Wait for processing
    time.sleep(10)

    prompt = """You are an expert mathematics curriculum extractor.
I have provided two PDF files. The first is a South African NSC Grade 12 Mathematics Question Paper. The second is the official Marking Guideline (Memo) for that exact paper.

Your task is to extract ONLY Question 1 and Question 2 from the Question Paper. 
For each sub-question (e.g. 1.1.1, 1.1.2), find the corresponding final answer and explanation from the Marking Guideline.

Format the output strictly as a JSON array of objects, with NO markdown wrapping, matching this schema:
[
  {
    "question_number": "1.1.1",
    "topic_name": "algebra",
    "sub_topic": "equations_quadratic",
    "question_text": "Solve for x: x^2 - x - 20 = 0",
    "correct_answer": "x = 5 or x = -4",
    "explanation": "Standard factorization: (x-5)(x+4)=0"
  }
]

IMPORTANT: You must escape all backslashes in your JSON strings. For example, use `\\\\( x^2 \\\\)` for MathJax, NOT `\\( x^2 \\)`. Failure to escape backslashes will break the JSON parser.
Use the following sub_topics based on the question: equations_linear, equations_quadratic, equations_simultaneous, inequalities, exponents_surds, nature_of_roots, logarithms, calculus_first_principles, calculus_rules, calculus_cubics, calculus_optimization.

Respond ONLY with the JSON array.
"""

    print("Requesting extraction from Gemini...")
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[q_file, m_file, prompt]
        )
        print("Raw response received.")
        
        text = response.text
        if text.startswith("```json"):
            text = text[7:-3]
        elif text.startswith("```"):
            text = text[3:-3]
        
        try:
            # Handle unescaped backslashes commonly returned in MathJax
            text = text.replace('\\', '\\\\')
            data = json.loads(text.strip())
        except Exception as json_e:
            print("JSON PARSE ERROR:", json_e)
            print("RAW TEXT:")
            print(text)
            return
        
        with app.app_context():
            for item in data:
                q = AdvMathQuestion(
                    topic_name=item.get("topic_name", "algebra"),
                    sub_topic=item.get("sub_topic"),
                    source_paper="DBE May-June 2025 Paper 1 Test",
                    question_type="long_form",
                    question_text=item.get("question_text"),
                    correct_answer=item.get("correct_answer"),
                    explanation=item.get("explanation")
                )
                db.session.add(q)
            db.session.commit()
            print(f"Successfully extracted and seeded {len(data)} questions!")
            
    except Exception as e:
        print("ERROR:", e)

if __name__ == "__main__":
    test_extract()
