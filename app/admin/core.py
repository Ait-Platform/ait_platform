
from app.models.payment import VoucherToken
from app.models.auth import AuthSubject
from app.utils.roles import is_admin
from flask import abort
from flask_login import current_user
# Cleaned admin core module
import os
import uuid
import base64
import json
import glob
import re
import google.generativeai as genai
from sqlalchemy import text
from flask import (
    render_template,
    request,
    jsonify,
    current_app,
    redirect,
    url_for,
    flash,
)
from . import admin_bp
from app.extensions import db
from app.models.auth import AuthSubject
from app.models.adv_math import AdvMathQuestion

@admin_bp.route("/", endpoint="index")
def index():
    allowed = ["reading", "home", "loss", "billing", "adv_math", "spv"]
    subjects = AuthSubject.query.filter(AuthSubject.slug.in_(allowed)).order_by(AuthSubject.name).all()
    return render_template("admin/index.html", subjects=subjects)

@admin_bp.route("/programs", methods=["GET", "POST"])
def manage_programs():
    if request.method == "POST":
        subject_id = request.form.get("subject_id")
        is_hidden = request.form.get("is_hidden") == "1"
        req_price = request.form.get("requires_price") == "1"
        ptype = request.form.get("program_type")
        subj = AuthSubject.query.get(subject_id)
        if subj:
            subj.is_hidden_on_bridge = is_hidden
            subj.requires_price = req_price
            subj.program_type = ptype
            db.session.commit()
            flash(f"Updated {subj.name}", "success")
        return redirect(url_for("admin_bp.manage_programs"))
    subjects = AuthSubject.query.order_by(AuthSubject.name).all()
    return render_template("admin/programs.html", subjects=subjects)

@admin_bp.route("/settings", methods=["GET", "POST"])
def global_settings():
    from sqlalchemy import text
    if request.method == "POST":
        quote_cents = request.form.get("mechanic_quote_cents")
        invoice_cents = request.form.get("mechanic_invoice_cents")
        enquiry_cents = request.form.get("practice_enquiry_cents")
        
        hds_cents = request.form.get("hds_subscription_cents")
        adv_reg_cents = request.form.get("adv_math_registration_cents")
        adv_sub_cents = request.form.get("adv_math_subtopic_cents")
        
        bil_base = request.form.get("bil_base_price")
        bil_inc = request.form.get("bil_included_meters")
        bil_extra = request.form.get("bil_extra_meter_price")
        
        updates = []
        if quote_cents: updates.append(('mechanic_quote_cents', quote_cents))
        if invoice_cents: updates.append(('mechanic_invoice_cents', invoice_cents))
        if enquiry_cents: updates.append(('practice_enquiry_cents', enquiry_cents))
        if hds_cents: updates.append(('hds_subscription_cents', hds_cents))
        if adv_reg_cents: updates.append(('adv_math_registration_cents', adv_reg_cents))
        if adv_sub_cents: updates.append(('adv_math_subtopic_cents', adv_sub_cents))
        
        for key, val in updates:
            db.session.execute(text("INSERT INTO system_settings (key, value) VALUES (:k, :v) ON CONFLICT(key) DO UPDATE SET value=excluded.value"), {"k": key, "v": val})
            
        from app.models.billing import BilPlatformSettings
        bil_settings = BilPlatformSettings.query.first()
        if not bil_settings:
            bil_settings = BilPlatformSettings(base_price_cents=10000, included_meters=2, extra_meter_price_cents=1500)
            db.session.add(bil_settings)
        if bil_base: bil_settings.base_price_cents = int(float(bil_base) * 100)
        if bil_inc: bil_settings.included_meters = int(bil_inc)
        if bil_extra: bil_settings.extra_meter_price_cents = int(float(bil_extra) * 100)
            
        db.session.commit()
        flash("Global settings updated successfully", "success")
        return redirect(url_for("admin_bp.global_settings"))
        
    settings = db.session.execute(text("SELECT key, value FROM system_settings")).fetchall()
    settings_dict = {s.key: s.value for s in settings}
    
    from app.models.billing import BilPlatformSettings
    bil_settings = BilPlatformSettings.query.first()
    if not bil_settings:
        bil_settings = BilPlatformSettings(base_price_cents=10000, included_meters=2, extra_meter_price_cents=1500)
        db.session.add(bil_settings)
        db.session.commit()
        
    return render_template("admin/settings.html", settings=settings_dict, bil_settings=bil_settings)

@admin_bp.route("/adv_math/extract", methods=["GET"], endpoint="adv_math_extract")
def adv_math_extract():
    return render_template("admin/adv_math/extract.html")

@admin_bp.route("/adv_math/extract_save", methods=["POST"], endpoint="adv_math_extract_save")
def adv_math_extract_save():
    try:
        data = request.get_json()
        image_data = data.get("image_data")
        source_paper = data.get("source_paper")
        topic_name = data.get("topic_name")
        sub_topic = data.get("sub_topic")
        concepts_tested = data.get("concepts_tested")
        marks = data.get("marks")
        if not all([image_data, source_paper, topic_name, marks]):
            return jsonify({"success": False, "error": "Missing required fields"}), 400
        header, encoded = image_data.split(",", 1)
        image_bytes = base64.b64decode(encoded)
        img_dir = os.path.join(current_app.static_folder, "images", "questions")
        os.makedirs(img_dir, exist_ok=True)
        filename = f"{uuid.uuid4().hex}.jpg"
        filepath = os.path.join(img_dir, filename)
        with open(filepath, "wb") as f:
            f.write(image_bytes)
        img_tag = f'<img src="/static/images/questions/{filename}" class="w-full max-w-2xl mx-auto rounded-lg shadow-sm border border-slate-200">'
        new_q = AdvMathQuestion(
            topic_name=topic_name,
            sub_topic=sub_topic,
            concepts_tested=concepts_tested,
            source_paper=source_paper,
            question_type="long_form",
            question_text=img_tag,
            marks=int(marks),
            correct_answer="Pending",
        )
        db.session.add(new_q)
        db.session.commit()
        return jsonify({"success": True, "question_id": new_q.id})
    except Exception as e:
        db.session.rollback()
        return jsonify({"success": False, "error": str(e)}), 500

@admin_bp.route("/adv_math/delete_question/<int:q_id>", methods=["POST"], endpoint="adv_math_delete_question")
def adv_math_delete_question(q_id):
    try:
        q = AdvMathQuestion.query.get(q_id)
        if not q:
            return jsonify({"success": False, "error": "Question not found"}), 404
        img_match = re.search(r'src="([^\"]+)"', q.question_text)
        if img_match:
            img_path = img_match.group(1)
            if img_path.startswith('/'):
                img_path = img_path[1:]
            full_path = os.path.join(current_app.root_path, img_path)
            if os.path.exists(full_path):
                os.remove(full_path)
        db.session.delete(q)
        db.session.commit()
        return jsonify({"success": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"success": False, "error": str(e)}), 500

@admin_bp.route("/adv_math/ai_scan_paper", methods=["POST"], endpoint="adv_math_ai_scan_paper")
def adv_math_ai_scan_paper():
    try:
        data = request.get_json()
        paper_name = data.get("paper_name")
        if not paper_name:
            return jsonify({"success": False, "error": "No paper selected"}), 400
        cache_dir = os.path.join(current_app.root_path, "data", "ai_scans")
        cache_file = os.path.join(cache_dir, f"{paper_name.replace(' ', '_')}.json")
        os.makedirs(cache_dir, exist_ok=True)
        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                return jsonify({"success": True, "map": json.load(f), "cached": True})
        data_dir = os.path.join(current_app.root_path, "data", "dbe_papers")
        pdf_files = glob.glob(os.path.join(data_dir, "*.pdf"))
        target_pdf = next(
            (
                p
                for p in pdf_files
                if paper_name.lower().replace("-", " ").replace("paper ", "p")
                in os.path.basename(p).lower().replace("-", " ")
            ),
            None,
        )
        if not target_pdf:
            return jsonify({"success": False, "error": "PDF not found"}), 404
        genai.configure(api_key=current_app.config.get("GEMINI_API_KEY", os.environ.get("GEMINI_API_KEY")))
        model = genai.GenerativeModel("gemini-2.0-flash")
        with open(target_pdf, "rb") as f:
            pdf_bytes = f.read()
        response = model.generate_content([{"mime_type": "application/pdf", "data": pdf_bytes}, "Extract exam questions as JSON array."])
        json_str = response.text.replace("```json", "").replace("```", "").strip()
        json_data = json.loads(json_str)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(json_data, f)
        return jsonify({"success": True, "map": json_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@admin_bp.route("/adv_math/extract_memo", methods=["GET"], endpoint="adv_math_extract_memo")
def adv_math_extract_memo():
    return render_template("admin/adv_math/extract_memo.html")

@admin_bp.route("/adv_math/api/questions_by_paper", methods=["POST"], endpoint="adv_math_api_questions_by_paper")
def adv_math_api_questions_by_paper():
    data = request.get_json()
    paper_name = data.get("paper_name")
    questions = AdvMathQuestion.query.filter_by(source_paper=paper_name).all()
    q_list = [
        {"id": q.id, "topic": q.topic_name, "sub_topic": q.sub_topic, "marks": q.marks}
        for q in questions
    ]
    return jsonify({"success": True, "questions": q_list})

@admin_bp.route("/adv_math/extract_memo_save", methods=["POST"], endpoint="adv_math_extract_memo_save")
def adv_math_extract_memo_save():
    try:
        data = request.get_json()
        question_id = data.get("question_id")
        image_data = data.get("image_data")
        if not question_id or not image_data:
            return jsonify({"success": False, "error": "Missing required fields"}), 400
        q = AdvMathQuestion.query.get(question_id)
        if not q:
            return jsonify({"success": False, "error": "Question not found"}), 404
        header, encoded = image_data.split(",", 1)
        image_bytes = base64.b64decode(encoded)
        img_dir = os.path.join(current_app.static_folder, "images", "memos")
        os.makedirs(img_dir, exist_ok=True)
        filename = f"memo_{uuid.uuid4().hex}.jpg"
        filepath = os.path.join(img_dir, filename)
        with open(filepath, "wb") as f:
            f.write(image_bytes)
        img_tag = f'<img src="/static/images/memos/{filename}" class="w-full max-w-2xl mx-auto rounded-lg shadow-sm border border-slate-200">'
        q.marking_memo = img_tag
        db.session.commit()
        return jsonify({"success": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/modules_control", methods=["GET", "POST"], endpoint="modules_control")
def modules_control():
    if request.method == "POST":
        updates = []
        for k, v in request.form.items():
            if k.startswith('visibility_') or k.startswith('yoco_mode_'):
                updates.append((k, v))
        for key, val in updates:
            db.session.execute(text("INSERT INTO system_settings (key, value) VALUES (:k, :v) ON CONFLICT(key) DO UPDATE SET value=excluded.value"), {"k": key, "v": val})
        db.session.commit()
        flash("Module controls updated successfully", "success")
        return redirect(url_for("admin_bp.modules_control"))
        
    settings = db.session.execute(text("SELECT key, value FROM system_settings")).fetchall()
    settings_dict = {s.key: s.value for s in settings}
    return render_template("admin/modules_control.html", settings=settings_dict)


@admin_bp.route('/vouchers', methods=['GET', 'POST'], endpoint='manage_vouchers')
def manage_vouchers():
    if not is_admin():
        abort(403)
        
    if request.method == 'POST':
        subject_id = request.form.get('subject_id', type=int)
        value_amount = request.form.get('value_amount', type=int)
        code = request.form.get('code')
        
        if not subject_id or not value_amount:
            flash('Subject and Value Amount are required.', 'danger')
        else:
            if not code:
                # Generate a random 8-character uppercase code
                code = str(uuid.uuid4()).upper()[:8]
            
            # Check if code exists
            exists = VoucherToken.query.filter_by(code=code).first()
            if exists:
                flash('That voucher code already exists!', 'danger')
            else:
                v = VoucherToken(
                    code=code, 
                    value_amount=value_amount, 
                    subject_id=subject_id,
                    created_by_user_id=current_user.id
                )
                db.session.add(v)
                db.session.commit()
                flash(f'Voucher {code} generated successfully!', 'success')
        return redirect(url_for('admin_bp.manage_vouchers'))

    # GET request
    vouchers = VoucherToken.query.order_by(VoucherToken.created_at.desc()).all()
    subjects = AuthSubject.query.order_by(AuthSubject.name.asc()).all()
    
    return render_template('admin/vouchers.html', vouchers=vouchers, subjects=subjects)

