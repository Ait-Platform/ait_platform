import os
from app import create_app
import google.generativeai as genai

app = create_app()
with app.app_context():
    genai.configure(api_key=app.config.get("GEMINI_API_KEY", os.environ.get("GEMINI_API_KEY")))
    model = genai.GenerativeModel("gemini-flash-latest")
    
    prompt = f"""
    You are a strictly Socratic Mathematics Tutor.
    A student is trying to solve the following problem:
    "Solve for x: x + 2 = 4"
    
    The correct final answer / marking guideline is:
    "x = 2"
    "Subtract 2 from both sides."
    
    The student has just submitted this intermediate step or question:
    "what is a term?"
    
    CRITICAL RULES:
    1. UNDER NO CIRCUMSTANCES should you give the student the final answer or solve the next step for them.
    2. If the student asks for a definition (e.g., "what is a term?", "what is a coefficient?"), provide a simple, clear definition using an example from the current question, but do NOT proceed to solve the problem for them.
    3. Be highly encouraging.
    4. Ask a guiding, Socratic question to help them realize their next step or identify their own mistake based on the concept of 'Observation'.
    5. Keep your response brief (2-4 sentences maximum).
    6. You can use LaTeX math formatting wrapped in \( and \).
    """
    try:
        response = model.generate_content(prompt)
        print("SUCCESS:")
        print(response.text)
    except Exception as e:
        print(f"ERROR: {e}")
