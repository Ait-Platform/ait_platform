import os
import json
import time
import fitz  # PyMuPDF
from PIL import Image
import google.generativeai as genai
from app import create_app
from app.extensions import db
from app.models.adv_math import AdvMathQuestion, AdvMathStep

def process_papers():
    app = create_app()
    genai.configure(api_key=app.config.get("GEMINI_API_KEY", os.environ.get("GEMINI_API_KEY")))
    model = genai.GenerativeModel("gemini-flash-latest") # Use standard flash model
    
    with app.app_context():
        # We no longer wipe the DB. We will resume from where we left off.
        print("Resuming extraction without wiping DB...")

    
    dbe_papers_dir = os.path.join(app.root_path, "data", "dbe_papers")
    if not os.path.exists(dbe_papers_dir):
        print(f"Directory {dbe_papers_dir} does not exist.")
        return

    # Load processed chunks to allow resuming
    processed_chunks_file = os.path.join(dbe_papers_dir, "processed_chunks.json")
    if os.path.exists(processed_chunks_file):
        with open(processed_chunks_file, "r") as f:
            processed_chunks = json.load(f)
    else:
        processed_chunks = []

    with app.app_context():
        for filename in os.listdir(dbe_papers_dir):
            if filename.lower().endswith('.pdf'):
                pdf_path = os.path.join(dbe_papers_dir, filename)
                print(f"Processing {filename}...")
                
                try:
                    doc = fitz.open(pdf_path)
                    total_pages = len(doc)
                    
                    # Process in chunks of 2 pages
                    for i in range(0, total_pages, 2):
                        chunk_id = f"{filename}_{i}"
                        if chunk_id in processed_chunks:
                            print(f"  Skipping chunk pages {i+1} to {min(i+2, total_pages)} (already processed).")
                            continue

                        print(f"  Chunking pages {i+1} to {min(i+2, total_pages)} of {total_pages}...")
                        
                        images = []
                        # Extract 2 pages
                        for j in range(i, min(i+2, total_pages)):
                            page = doc.load_page(j)
                            matrix = fitz.Matrix(2.0, 2.0)
                            pix = page.get_pixmap(matrix=matrix)
                            
                            # Convert to PIL Image
                            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                            images.append(img)
                            
                        # Prepare Gemini prompt
                        prompt = f"""
You are an expert South African DBE Mathematics examiner.
I have provided images of {len(images)} pages from an official DBE Mathematics Exam Paper ({filename}).

Your task is to extract all the mathematical questions found on these pages and provide a detailed marking guideline (answer) for each.

CRITICAL RULES:
1. Do NOT alter the format of the question. If it is an MCQ, extract the options. If it is a long-form question, leave the options blank and extract the full text.
2. If a question relies on a diagram or graph that you cannot represent in text, you must describe the diagram mathematically in the question text so the student can still solve it.
3. Classify EVERY question into ONE of the following broad topics ONLY: "algebra", "functions", "financial_math", "calculus", "probability", "geometry".
4. Classify EVERY question into ONE of the following specific sub-topics ONLY (choose the best fit, ensuring it matches the broad topic):
   - Algebra: "equations_inequalities", "sequences_arithmetic", "sequences_geometric", "sequences_quadratic", "exponents_surds", "nature_of_roots", "logarithms"
   - Functions & Calculus: "functions_linear_quad", "functions_exp_log", "functions_hyperbolas", "functions_inverses", "calculus_first_principles", "calculus_rules", "calculus_cubics", "calculus_optimization"
   - Probability & Stats: "probability_counting", "probability_diagrams", "statistics_regression", "statistics_distribution"
   - Geometry & Trig: "euclidean_circle_theorems", "euclidean_proportionality", "analytical_lines_circles", "trigonometry_identities", "trigonometry_graphs", "trigonometry_2d_3d"
   - Financial Math: "financial_interest", "financial_annuities"
   - Or "other" if none apply.
5. You MUST wrap ALL mathematical formulas, variables, and expressions in LaTeX delimiters `\\(` and `\\)` so they can be rendered by MathJax. (e.g. `Solve for \\( x \\): \\( x^2 - 4x - 5 = 0 \\)`).
6. Return ONLY a raw JSON array matching this exact schema:
[
  {{
    "question_text": "...",
    "question_type": "long_form", // or "mcq"
    "options": ["A", "B", "C", "D"], // leave empty if long_form
    "marks": 5, // IMPORTANT: The exact numerical mark allocation for this question (e.g. 5 if it says [5]). Default to 0 if not found.
    "correct_answer": "...", // The final answer
    "marking_memo": "...", // The complete unbroken marking memo
    "steps": [ // CRITICAL: Break the problem down like a Socratic tutor! Do NOT jump to the final answer or skip intermediate calculations (like finding 'n'). Break the solution into extremely granular, sequential micro-steps.
       // VERY IMPORTANT: Your instructions must guide the student to discover the next step WITHOUT giving away the name of the mathematical rule, theorem, or technique (e.g. Do NOT say 'Recognize the difference of squares' or 'Use the quadratic formula'. INSTEAD say 'What do you notice about the two terms?' or 'What formula can we use to find the roots here?').
       // E.g., for sequences, Step 1: "What are the known variables?", Step 2: "What formula connects these variables?", Step 3: "Substitute the variables to find n", Step 4: "What is the final sum formula?", Step 5: "Calculate the final answer".
       {{"step_number": 1, "instruction": "A highly specific question prompting the student for the next micro-step (e.g. 'What are the known variables \\(a\\), \\(d\\), and the last term \\(L\\)?')", "math_content": "The explicit math or variables for this step (e.g. '\\(a=5, d=2, L=93\\)')"}},
       {{"step_number": 2, "instruction": "The next question prompting the next micro-step", "math_content": "..."}}
    ],
    "topic": "algebra", // MUST be from the broad topics list
    "sub_topic": "sequences_series" // MUST be from the specific sub-topics list
  }}
]

If there are NO mathematical questions on these pages (e.g. it's a cover page or formula sheet), return an empty array [].
Do NOT wrap the output in markdown block ticks. Return raw JSON.
"""
                        contents = images + [prompt]
                        
                        max_retries = 5
                        for attempt in range(max_retries):
                            try:
                                print(f"    Calling Gemini API (Attempt {attempt+1})...")
                                response = model.generate_content(contents)
                                raw_text = response.text.strip()
                                
                                # Clean up markdown if Gemini ignored instructions
                                if raw_text.startswith("```json"):
                                    raw_text = raw_text.split("```json")[1].split("```")[0].strip()
                                elif raw_text.startswith("```"):
                                    raw_text = raw_text.split("```")[1].split("```")[0].strip()
                                    
                                data = json.loads(raw_text)
                                
                                if not data:
                                    print("    No questions found on these pages.")
                                break # success, break retry loop
                            except Exception as e:
                                print(f"    Gemini API or Parsing Error on attempt {attempt+1}: {e}")
                                if attempt < max_retries - 1:
                                    import time
                                    print("    Waiting 65 seconds before retrying (rate limit buffer)...")
                                    time.sleep(65)
                                else:
                                    data = [] # give up
                                    
                        if not data:
                            continue
                            
                        # Save to DB
                        total_marks_this_chunk = 0
                        for q in data:
                            q_obj = AdvMathQuestion(
                                topic_name=q["topic"],
                                sub_topic=q.get("sub_topic", "other"),
                                source_paper=filename,
                                question_type=q["question_type"],
                                question_text=q["question_text"],
                                marks=q.get("marks", 0),
                                option_a=q.get("options", ["", "", "", ""])[0] if q.get("options") else "",
                                option_b=q.get("options", ["", "", "", ""])[1] if q.get("options") else "",
                                option_c=q.get("options", ["", "", "", ""])[2] if q.get("options") else "",
                                option_d=q.get("options", ["", "", "", ""])[3] if q.get("options") else "",
                                correct_answer=q.get("correct_answer", ""),
                                explanation=q.get("explanation", ""),
                                marking_memo=q.get("marking_memo", "")
                            )
                            db.session.add(q_obj)
                            db.session.flush() # flush to get the id for the steps
                            
                            total_marks_this_chunk += q.get("marks", 0)
                            
                            for s in q.get("steps", []):
                                step_obj = AdvMathStep(
                                    question_id=q_obj.id,
                                    step_number=s.get("step_number", 1),
                                    instruction=s.get("instruction", ""),
                                    math_content=s.get("math_content", "")
                                )
                                db.session.add(step_obj)
                                
                        db.session.commit()
                        
                        # Mark chunk as processed
                        processed_chunks.append(chunk_id)
                        with open(processed_chunks_file, "w") as f:
                            json.dump(processed_chunks, f)

                        print(f"    Successfully extracted and saved {len(data)} questions. Chunk Marks: {total_marks_this_chunk}")
                        
                        # Respect rate limits (15 RPM for Free Tier)
                        print("    Waiting 15 seconds to respect rate limits...")
                        time.sleep(15)
                        
                except Exception as e:
                    print(f"Failed to process PDF {filename}: {e}")

if __name__ == "__main__":
    process_papers()
    print("Done processing all papers.")
