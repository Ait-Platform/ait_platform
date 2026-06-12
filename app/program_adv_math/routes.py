import os
import uuid
import json
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, session, jsonify
from flask_login import login_required, current_user, login_user

from app.extensions import db
from app.models.auth import User, UserEnrollment, AuthSubject

from app.models.adv_math import AdvMathProgress, AdvMathAssessment
from werkzeug.security import generate_password_hash
import re

adv_math_bp = Blueprint("adv_math_bp", __name__, url_prefix="/adv-math")

MATH_GLOSSARY = {
    "term": {"definition": "A single number, a variable, or numbers and variables multiplied together (e.g., \\(4x\\) or \\(7\\)).", "plain": "Think of a 'term' as a single chunk of math separated by plus or minus signs."},
    "coefficient": {"definition": "The number multiplied by a variable. In \\(5x\\), 5 is the coefficient.", "plain": "The number sitting right next to the letter, telling you how many of that letter you have."},
    "quadratic": {"definition": "An equation where the highest exponent of the variable is 2 (e.g., \\(x^2 + 3x + 2 = 0\\)).", "plain": "An equation that has a squared letter, like x², which usually forms a U-shape graph."},
    "factorize": {"definition": "Finding what to multiply together to get an expression. The reverse of expanding.", "plain": "Breaking a big math expression down into pieces that multiply together to make it, like reverse engineering."},
    "sequence": {"definition": "A list of numbers or objects in a special order.", "plain": "A pattern of numbers lined up in a row."},
    "geometric": {"definition": "A sequence made by multiplying by the same value each time (e.g., 2, 4, 8, 16).", "plain": "A pattern where you multiply the previous number by the exact same amount to get the next one."},
    "arithmetic": {"definition": "A sequence made by adding the same value each time (e.g., 1, 4, 7, 10).", "plain": "A pattern where you add or subtract the exact same amount to get to the next number."},
    "exponent": {"definition": "A quantity representing the power to which a given number or expression is to be raised.", "plain": "The tiny number at the top right that tells you how many times to multiply the big number by itself."},
    "variable": {"definition": "A symbol for a value we don't know yet, usually a letter like \\(x\\) or \\(y\\).", "plain": "A placeholder letter (like x) for a number we are trying to find."},
    "equation": {"definition": "A mathematical statement that two things are equal, containing an equals sign \\(=\\).", "plain": "A math sentence showing that the left side balances perfectly with the right side."},
    "expression": {"definition": "Numbers, symbols and operators grouped together that show the value of something.", "plain": "A math phrase without an equals sign."},
    "function": {"definition": "A special relationship where each input has a single output.", "plain": "A math machine: you drop one number in, and exactly one number pops out."},
    "limit": {"definition": "The value that a function approaches as the input approaches some value.", "plain": "Predicting where a graph is heading, even if it never perfectly touches that spot."},
    "derivative": {"definition": "The rate of change of a function with respect to a variable. The slope of the tangent.", "plain": "A formula that tells you exactly how steep a curve is at any specific point."},
    "probability": {"definition": "How likely something is to happen.", "plain": "The odds or chances of an event taking place."},
    "logarithm": {"definition": "The power to which a base must be raised to produce a given number.", "plain": "The reverse of an exponent. It asks: 'How many times do I multiply the base to get this number?'"},
    "asymptote": {"definition": "A line that a curve approaches, as it heads towards infinity.", "plain": "An invisible barrier line that the graph gets infinitely close to, but never touches."},
    "tangent": {"definition": "A line that just touches a curve at one point, without intersecting it.", "plain": "A straight line that lightly skims the edge of a curve at exactly one point."}
}

MATH_FORMULAS = {
    "quadratic": {"formula": "Quadratic Formula: \\( x = \\frac{-b \\pm \\sqrt{b^2 - 4ac}}{2a} \\)", "plain": "Plug your a, b, and c numbers into this to find the x-intercepts without having to factorize!"},
    "geometric": {"formula": "nth term: \\( T_n = ar^{n-1} \\)  |  Sum: \\( S_n = \\frac{a(r^n - 1)}{r - 1} \\)", "plain": "To find any number in the pattern, multiply the first number (a) by the constant ratio (r) over and over. 'r' is simply the multiplier you use to get from one number to the next (e.g., if the pattern is 2, 6, 18, 54... r is 3)."},
    "arithmetic": {"formula": "nth term: \\( T_n = a + (n-1)d \\)  |  Sum: \\( S_n = \\frac{n}{2}[2a + (n-1)d] \\)", "plain": "To find a number, start at (a) and keep adding the constant difference (d)."},
    "distance": {"formula": "Distance: \\( d = \\sqrt{(x_2 - x_1)^2 + (y_2 - y_1)^2} \\)", "plain": "Just a fancy way of using Pythagoras' theorem to find how long a line is between two points."},
    "gradient": {"formula": "Gradient: \\( m = \\frac{y_2 - y_1}{x_2 - x_1} \\)", "plain": "The steepness of the line: how much it goes up divided by how much it goes across (rise over run)."},
    "midpoint": {"formula": "Midpoint: \\( M(x,y) = \\left( \\frac{x_1 + x_2}{2}, \\frac{y_1 + y_2}{2} \\right) \\)", "plain": "Just find the average of the x's and the average of the y's to get the exact middle!"},
    "circle": {"formula": "Circle at origin: \\( x^2 + y^2 = r^2 \\)  |  Circle at (a,b): \\( (x-a)^2 + (y-b)^2 = r^2 \\)", "plain": "The equation that draws a perfect circle. r is the radius."}
}

MATH_RULES = {
    "exponent": {"rule": "1. \\(a^m \\times a^n = a^{m+n}\\)\n2. \\(a^m \\div a^n = a^{m-n}\\)\n3. \\((a^m)^n = a^{mn}\\)", "plain": "When multiplying same bases, add the tiny numbers. When dividing, subtract them!"},
    "logarithm": {"rule": "1. \\(\\log_a(xy) = \\log_a x + \\log_a y\\)\n2. \\(\\log_a(x/y) = \\log_a x - \\log_a y\\)\n3. \\(\\log_a(x^n) = n\\log_a x\\)", "plain": "Logs turn multiplication into addition, and division into subtraction."},
    "probability": {"rule": "1. \\(P(A \\cup B) = P(A) + P(B) - P(A \\cap B)\\)\n2. Mutually Exclusive: \\(P(A \\cap B) = 0\\)\n3. Independent: \\(P(A \\cap B) = P(A) \\times P(B)\\)", "plain": "If two events can't happen at the same time, you just add their chances together!"},
    "derivative": {"rule": "Power Rule: If \\(f(x) = ax^n\\), then \\(f'(x) = anx^{n-1}\\)", "plain": "Bring the exponent down to the front and multiply, then subtract 1 from the exponent."}
}

def get_math_progress(user_id, enrollment_id):
    prog = AdvMathProgress.query.filter_by(user_id=user_id, enrollment_id=enrollment_id).first()
    if not prog:
        prog = AdvMathProgress(user_id=user_id, enrollment_id=enrollment_id)
        db.session.add(prog)
        db.session.commit()
    return prog

@adv_math_bp.route("/")
@adv_math_bp.route("/about")
def about():
    return render_template("program_adv_math/about.html")

@adv_math_bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        name = request.form.get("name")
        email = request.form.get("email", "").lower().strip()
        password = request.form.get("password")
        province = request.form.get("province")
        is_private = request.form.get("is_private")

        if not name or not email or not password or not is_private:
            flash("Please fill all required fields and confirm you are a private candidate.", "error")
            return redirect(url_for("adv_math_bp.register"))

        user = User.query.filter_by(email=email).first()
        if not user:
            user = User(
                name=name,
                email=email,
                password_hash=generate_password_hash(password)
            )
            # Store province somewhere if needed, but User model doesn't have it explicitly yet.
            db.session.add(user)
            db.session.commit()
        
        login_user(user)
        return redirect(url_for("adv_math_bp.payflow"))

    return render_template("program_adv_math/register.html")

@adv_math_bp.route("/payflow")
def payflow():
    subj = AuthSubject.query.filter_by(slug="adv_math").first_or_404()
    
    # Check if already enrolled
    if getattr(current_user, "is_authenticated", False):
        enr = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=subj.id).first()
        if enr:
            flash("You are already enrolled in Advanced Mathematics.", "info")
            return redirect(url_for("adv_math_bp.dashboard"))

    return render_template("program_adv_math/payflow.html", subject=subj)

@adv_math_bp.route("/enroll/free", methods=["POST"])
@login_required
def enroll_free():
    # If the user chooses a free trial or we override for dev purposes
    subj = AuthSubject.query.filter_by(slug="adv_math").first_or_404()
    enr = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=subj.id).first()
    if not enr:
        from app.models.culturalfire import CfiBiodata
        dummy_bio = CfiBiodata.query.filter_by(user_id=current_user.id).first()
        if not dummy_bio:
            dummy_bio = CfiBiodata(user_id=current_user.id)
            db.session.add(dummy_bio)
            db.session.flush()
            
        enr = UserEnrollment(
            user_id=current_user.id, 
            subject_id=subj.id, 
            status="active",
            local_currency="ZAR",
            local_amount_cents=0,
            zar_amount_cents=0,
            biodata_id=dummy_bio.id
        )
        db.session.add(enr)
    else:
        enr.status = "active"
        
    db.session.commit()
    
    flash("Successfully enrolled in Advanced Mathematics!", "success")
    return redirect(url_for("adv_math_bp.dashboard"))

@adv_math_bp.route("/dashboard")
@login_required
def dashboard():
    subj = AuthSubject.query.filter_by(slug="adv_math").first_or_404()
    enr = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=subj.id).first()
    if not enr:
        return redirect(url_for("adv_math_bp.about"))
        
    prog = get_math_progress(current_user.id, enr.id)
    
    # Calculate progress %
    topics = [prog.topic_numbers, prog.topic_algebra, prog.topic_functions, prog.topic_calculus, prog.topic_probability, prog.topic_geometry]
    mastered_count = sum(1 for t in topics if t == "mastered")
    progress_pct = int((mastered_count / 6.0) * 100)
    
    # Auto-update readiness
    if mastered_count == 6:
        prog.readiness_status = 2
    elif mastered_count >= 4:
        prog.readiness_status = 1
    else:
        prog.readiness_status = 0
    db.session.commit()

    return render_template("program_adv_math/dashboard.html", progress=prog, progress_pct=progress_pct, enrollment=enr)

@adv_math_bp.route("/topic/<topic_id>")
@adv_math_bp.route("/topic/<topic_id>/<sub_topic>")
@login_required
def topic_flow(topic_id, sub_topic=None):
    user_id = current_user.id
    enrollments = UserEnrollment.query.filter_by(user_id=user_id, status='active').all()
    if not enrollments:
        flash("You need an active enrollment to access this module.", "warning")
        return redirect(url_for('adv_math_bp.about'))
    
    enrollment_id = enrollments[0].id
    prog = get_math_progress(user_id, enrollment_id)
    
    from app.models.adv_math import AdvMathQuestion
    import random
    
    query = AdvMathQuestion.query.filter_by(topic_name=topic_id)
    if sub_topic:
        query = query.filter_by(sub_topic=sub_topic)
    topic_questions = query.order_by(AdvMathQuestion.id.desc()).all()
    
    is_variation = request.args.get("variation") == "true"
    variation_key = f"adv_math_variation_{topic_id}_{sub_topic}"
    
    pagination = {}
    if is_variation and variation_key in session:
        var_data = session[variation_key]
        question = {
            "text": var_data["text"],
            "options": [],
            "topic": topic_id,
            "type": "open",
            "source": "Mastery Challenge",
            "marks": "12",
            "available_tools": ["formulas", "strategy", "steps", "deconstruct", "tools"]
        }
        
        session[f"adv_math_q_{topic_id}"] = {
            "id": "variation",
            "correct_answer": var_data["correct_answer"],
            "explanation": var_data["explanation"],
            "text": var_data["text"],
            "a": var_data.get("a", "2"),
            "r": var_data.get("r", "3"),
            "n": var_data.get("n", "10")
        }
    elif topic_questions:
        total_questions = len(topic_questions)
        qid = request.args.get('qid')
        q_obj = None
        
        if qid:
            for i, q in enumerate(topic_questions):
                if str(q.id) == str(qid):
                    q_obj = q
                    current_index = i
                    break
                    
        if not q_obj:
            q_obj = topic_questions[0]
            current_index = 0
            
        prev_qid = topic_questions[current_index - 1].id if current_index > 0 else None
        next_qid = topic_questions[current_index + 1].id if current_index < total_questions - 1 else None
        
        pagination = {
            "current_index": current_index + 1,
            "total_questions": total_questions,
            "prev_qid": prev_qid,
            "next_qid": next_qid,
            "questions_list": topic_questions
        }
        
        options = []
        if q_obj.question_type == "mcq":
            options = [opt for opt in [q_obj.option_a, q_obj.option_b, q_obj.option_c, q_obj.option_d] if opt]
            
        question = {
            "text": q_obj.question_text,
            "options": options,
            "topic": topic_id,
            "type": q_obj.question_type,
            "source": q_obj.source_paper,
            "marks": "5",
            "available_tools": ["definitions", "formulas", "strategy", "deconstruct"]
        }
        
        session[f"adv_math_q_{topic_id}"] = {
            "id": q_obj.id,
            "correct_answer": q_obj.correct_answer,
            "explanation": q_obj.explanation,
            "text": q_obj.question_text
        }
    else:
        question = {
            "text": f"Evaluate the limit as x approaches 0 for sin(x)/x (Mock DBE Question - {topic_id.capitalize()})",
            "options": ["0", "1", "Infinity", "Undefined"],
            "topic": topic_id,
            "type": "mcq",
            "source": "Mock Data"
        }
        session[f"adv_math_q_{topic_id}"] = {
            "correct_answer": "1",
            "explanation": "By L'Hopital's rule, the derivative of sin(x) is cos(x) and x is 1. cos(0)/1 = 1.",
            "text": question["text"]
        }
        
    active_definitions = []
    text_to_search = question["text"].lower()
    for term, data in MATH_GLOSSARY.items():
        if re.search(r'\b' + term + r'\b', text_to_search):
            active_definitions.append({
                "term": term.capitalize(),
                "definition": data["definition"],
                "plain": data["plain"]
            })
            
    if not active_definitions:
        default_terms = ["term", "coefficient", "equation"]
        for dt in default_terms:
            active_definitions.append({
                "term": dt.capitalize(),
                "definition": MATH_GLOSSARY[dt]["definition"],
                "plain": MATH_GLOSSARY[dt]["plain"]
            })
            
    active_formulas = []
    for term, data in MATH_FORMULAS.items():
        if re.search(r'\b' + term + r'\b', text_to_search):
            active_formulas.append({
                "term": term.capitalize(),
                "formula": data["formula"],
                "plain": data["plain"]
            })
            
    active_rules = []
    for term, data in MATH_RULES.items():
        if re.search(r'\b' + term + r'\b', text_to_search):
            active_rules.append({
                "term": term.capitalize(),
                "rule": data["rule"],
                "plain": data["plain"]
            })
    
    return render_template(
        "program_adv_math/topic_flow.html",
        topic_id=topic_id,
        sub_topic=sub_topic,
        topic_title=topic_id.replace('_', ' ').title(),
        question=question,
        progress=prog,
        pagination=pagination,
        active_definitions=active_definitions,
        active_formulas=active_formulas,
        active_rules=active_rules,
        is_variation=is_variation
    )

@adv_math_bp.route("/topic/<topic_id>/<sub_topic>/variation", methods=["GET"])
@login_required
def generate_variation(topic_id, sub_topic):
    import random
    if sub_topic == "sequences_geometric":
        a = random.randint(2, 5)
        r = random.randint(2, 4)
        n = random.randint(5, 8)
        
        t1 = a
        t2 = a * r
        t3 = a * (r**2)
        t4 = a * (r**3)
        
        correct_sum = int(a * ((r**n) - 1) / (r - 1))
        
        question_text = f"Consider the geometric sequence: {t1}, {t2}, {t3}, {t4}... Determine the nth term and calculate the sum of the first {n} terms."
        
        session[f"adv_math_variation_{topic_id}_{sub_topic}"] = {
            "text": question_text,
            "correct_answer": str(correct_sum),
            "explanation": "Mastery challenge solved using standard heuristic method.",
            "a": str(a),
            "r": str(r),
            "n": str(n)
        }
        return redirect(url_for("adv_math_bp.topic_flow", topic_id=topic_id, sub_topic=sub_topic, variation="true"))
    
    return redirect(url_for("adv_math_bp.topic_flow", topic_id=topic_id, sub_topic=sub_topic))

@adv_math_bp.route("/topic/<topic_id>/tutor", methods=["POST"])
@adv_math_bp.route("/topic/<topic_id>/<sub_topic>/tutor", methods=["POST"])
@login_required
def topic_tutor(topic_id, sub_topic=None):
    import os
    import json
    import google.generativeai as genai
    
    q_data = session.get(f"adv_math_q_{topic_id}")
    if not q_data:
        return {"error": "Session expired or no active question."}, 400
        
    correct_answer = q_data.get("correct_answer", "")
    explanation = q_data.get("explanation", "")
    question_text = q_data.get("text", "")
    
    data = request.get_json()
    student_step = data.get("step", "").strip()
    
    if not student_step:
        return {"error": "Please provide your working or question."}, 400
        
    try:
        genai.configure(api_key=current_app.config.get("GEMINI_API_KEY", os.environ.get("GEMINI_API_KEY")))
        model = genai.GenerativeModel("gemini-flash-latest")
        
        prompt = f"""
        You are a strictly Socratic Mathematics Tutor.
        A student is trying to solve the following problem:
        "{question_text}"
        
        The correct final answer / marking guideline is:
        "{correct_answer}"
        "{explanation}"
        
        The student has just submitted this intermediate step or question:
        "{student_step}"
        
        CRITICAL RULES:
        1. UNDER NO CIRCUMSTANCES should you give the student the final answer or solve the next step for them.
        2. If the student asks for a definition (e.g., "what is a term?", "what is a coefficient?"), provide a simple, clear definition using an example from the current question, but do NOT proceed to solve the problem for them.
        3. Be highly encouraging.
        4. Ask a guiding, Socratic question to help them realize their next step or identify their own mistake based on the concept of 'Observation'.
        5. Keep your feedback brief (2-4 sentences maximum).
        6. You can use LaTeX math formatting wrapped in \\\\( and \\\\).
        
        You MUST return your response as a raw JSON object matching this exact schema:
        {
            "feedback": "Your Socratic, encouraging response here.",
            "is_final_answer_reached": true/false
        }
        Set "is_final_answer_reached" to true ONLY if the student's step logically demonstrates the final correct answer based on the marking guideline. Otherwise, set it to false. Do not wrap the JSON in markdown ticks.
        """
        
        response = model.generate_content(prompt)
        raw_text = response.text.strip()
        
        if raw_text.startswith("```json"):
            raw_text = raw_text.split("```json")[1].split("```")[0].strip()
        elif raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1].split("```")[0].strip()
            
        try:
            result_data = json.loads(raw_text)
        except Exception:
            result_data = {"feedback": raw_text, "is_final_answer_reached": False}
        
        return {
            "feedback": result_data.get("feedback", "Keep going!"),
            "is_final": result_data.get("is_final_answer_reached", False)
        }
        
    except Exception as e:
        current_app.logger.error(f"Gemini Tutor API Error: {e}")
        
        # Fallback dynamic step-by-step
        a = int(q_data.get("a", "2"))
        r = int(q_data.get("r", "3"))
        n = int(q_data.get("n", "10"))
        
        fallback_msg = f"""
        The AI Tutor is overwhelmed by traffic right now! But don't let that stop you. 
        Here is the exact heuristic breakdown to solve this sequence manually:
        
        <ul class="list-disc ml-5 mt-2 space-y-1">
            <li><b>Variables:</b> a = {a}, r = {r}, n = {n}</li>
            <li><b>Step 1 (Exponent):</b> r^n = {r**n}</li>
            <li><b>Step 2 (Parenthesis):</b> r^n - 1 = {(r**n) - 1}</li>
            <li><b>Step 3 (Numerator):</b> a(r^n - 1) = {a * ((r**n) - 1)}</li>
            <li><b>Step 4 (Denominator):</b> r - 1 = {r - 1}</li>
            <li><b>Final Division:</b> {a * ((r**n) - 1)} / {r - 1} = <b>{int(a * ((r**n) - 1) / (r - 1))}</b></li>
        </ul>
        
        Review these steps, then confidently submit your final answer below!
        """
        
        return {"error": fallback_msg}, 200

@adv_math_bp.route("/validate-variables", methods=["POST"])
@login_required
def validate_variables():
    try:
        data = request.get_json() or {}
        a = str(data.get('a', '')).strip()
        r = str(data.get('r', '')).strip()
        n = str(data.get('n', '')).strip()
        topic_id = data.get('topic_id', 'sequences')
        sub_topic = data.get('sub_topic', 'sequences_geometric')
        is_var = data.get('is_variation', False)
        
        if is_var:
            sess_data = session.get(f"adv_math_variation_{topic_id}_{sub_topic}", {})
        else:
            sess_data = session.get(f"adv_math_q_{topic_id}", {})
            
        correct_a = sess_data.get("a", "2")
        correct_r = sess_data.get("r", "3")
        correct_n = sess_data.get("n", "10")
        
        is_a_correct = (a == correct_a)
        is_r_correct = (r == correct_r)
        is_n_correct = (n == correct_n)
        
        ca = int(correct_a)
        cr = int(correct_r)
        cn = int(correct_n)
        
        return jsonify({
            "a": is_a_correct,
            "r": is_r_correct,
            "n": is_n_correct,
            "all_correct": (is_a_correct and is_r_correct and is_n_correct),
            "ans_rn": str(cr ** cn),
            "ans_paren": str((cr ** cn) - 1),
            "ans_num": str(ca * ((cr ** cn) - 1)),
            "ans_den": str(cr - 1),
            "ans_final": str(int(ca * ((cr ** cn) - 1) / (cr - 1)))
        })
    except Exception as e:
        current_app.logger.error(f"Validate Variables Error: {e}")
        return jsonify({"error": str(e)}), 500

@adv_math_bp.route("/topic/<topic_id>/submit", methods=["POST"])
@adv_math_bp.route("/topic/<topic_id>/<sub_topic>/submit", methods=["POST"])
@login_required
def topic_submit(topic_id, sub_topic=None):
    subj = AuthSubject.query.filter_by(slug="adv_math").first_or_404()
    enr = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=subj.id).first_or_404()
    prog = get_math_progress(current_user.id, enr.id)
    
    answer = request.form.get("answer")
    
    # Retrieve expected answer from session
    q_data = session.pop(f"adv_math_q_{topic_id}", None)
    if not q_data:
        flash("Session expired or invalid question. Please try again.", "error")
        return redirect(url_for("adv_math_bp.topic_flow", topic_id=topic_id))
        
    correct_answer = q_data.get("correct_answer")
    explanation = q_data.get("explanation")
    question_text = q_data.get("text")
    
    is_correct = False
    ai_feedback = ""
    
    # Try exact match first
    if str(answer).strip().lower() == str(correct_answer).strip().lower():
        is_correct = True
        ai_feedback = f"Correct! {explanation}"
    else:
        # Try heuristic number extraction first (bypasses API limits)
        import re
        # Find all numbers in the student's text
        numbers = re.findall(r'-?\d+(?:\.\d+)?', str(answer))
        
        # Check if they found the correct sum
        has_correct_sum = str(correct_answer) in numbers
        
        # Check if they provided a reasonable representation of the nth term
        has_nth_term = False
        a_str = q_data.get("a", "")
        r_str = q_data.get("r", "")
        if a_str and r_str:
            # Look for a(r) or a*r or similar in the text
            if f"{a_str}({r_str})" in answer.replace(" ", "") or f"{a_str}*{r_str}" in answer.replace(" ", "") or f"r={r_str}" in answer.replace(" ", ""):
                has_nth_term = True
        else:
            # If a and r aren't available, just assume true for this heuristic
            has_nth_term = True
            
        if has_correct_sum and has_nth_term:
            is_correct = True
            ai_feedback = f"Correct! I see you found both the sequence pattern and the final sum ({correct_answer}). Great job! {explanation}"
        elif has_correct_sum:
            is_correct = True
            ai_feedback = f"Correct! You arrived at the right sum ({correct_answer}), though make sure you clearly state your nth term formula next time. {explanation}"
        else:
            # Use Gemini to grade open-ended answers as a last resort
            import os
            import google.generativeai as genai
            
            try:
                genai.configure(api_key=current_app.config.get("GEMINI_API_KEY", os.environ.get("GEMINI_API_KEY")))
                model = genai.GenerativeModel("gemini-flash-latest")
                
                eval_prompt = f"""
                You are a Mathematics teacher grading a student's answer.
                Question: {question_text}
                Correct Answer / Marking Guideline: {correct_answer}
                Detailed Explanation: {explanation}
                
                Student's Answer: {answer}
                
                Is the student's answer mathematically correct based on the marking guideline? 
                Respond with ONLY "YES" or "NO" on the first line. 
                Then, on the next line, provide a short 1-sentence feedback to the student.
                """
                response = model.generate_content(eval_prompt)
                result_text = response.text.strip().split("\n")
                
                if result_text and "YES" in result_text[0].upper():
                    is_correct = True
                    
                ai_feedback = " ".join(result_text[1:]) if len(result_text) > 1 else response.text
                
                if not is_correct and not ai_feedback:
                    ai_feedback = f"Incorrect. The expected answer is related to: {correct_answer}. {explanation}"
                    
            except Exception as e:
                current_app.logger.error(f"Gemini Grading Error: {e}")
                is_correct = False
                ai_feedback = f"Could not auto-grade your response due to high traffic. The correct answer is: {correct_answer}. {explanation}"
    
    # Record attempt
    attempt = AdvMathAssessment(
        user_id=current_user.id,
        topic_name=topic_id,
        question_text=question_text,
        student_answer=answer,
        is_correct=is_correct,
        ai_feedback=ai_feedback
    )
    db.session.add(attempt)
    
    if is_correct:
        setattr(prog, f"topic_{topic_id}", "mastered")
    else:
        setattr(prog, f"topic_{topic_id}", "in_progress")
        
    db.session.commit()
    
    if q_data.get("id"):
        session[f"last_qid_{topic_id}"] = q_data.get("id")
    
    return redirect(url_for("adv_math_bp.result", topic_id=topic_id, attempt_id=attempt.id))

@adv_math_bp.route("/result/<topic_id>/<int:attempt_id>")
@login_required
def result(topic_id, attempt_id):
    attempt = AdvMathAssessment.query.get_or_404(attempt_id)
    if attempt.user_id != current_user.id:
        return redirect(url_for("adv_math_bp.dashboard"))
        
    last_qid = session.get(f"last_qid_{topic_id}")
        
    return render_template("program_adv_math/result.html", topic_id=topic_id, attempt=attempt, last_qid=last_qid)

@adv_math_bp.route("/readiness")
@login_required
def readiness():
    subj = AuthSubject.query.filter_by(slug="adv_math").first_or_404()
    enr = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=subj.id).first_or_404()
    prog = get_math_progress(current_user.id, enr.id)
    
    return render_template("program_adv_math/readiness.html", progress=prog)
