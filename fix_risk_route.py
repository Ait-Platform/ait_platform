import re
content = open('app/program_healthcore/routes.py', 'r').read()

risk_route = '''
@healthcore_bp.route("/program/healthcore/engine/risk/generate", methods=["POST"])
@login_required
@healthcore_onboarded_required
def generate_risk():
    from app.program_healthcore.ai_extractor import generate_risk_assessment
    result = generate_risk_assessment(current_user.id)
    if "error" in result:
        flash(f"Error generating Risk Assessment: {result['error']}", "danger")
    else:
        flash("AI Risk Assessment generated successfully!", "success")
    return redirect(url_for("healthcore_bp.risk_dashboard"))
'''

content = content.replace('def risk_dashboard():\n    from app.models.healthcore import HcRiskAssessment\n    records = HcRiskAssessment.query.filter_by(user_id=current_user.id).order_by(HcRiskAssessment.calculated_date.desc(), HcRiskAssessment.created_at.desc()).all()\n    return render_template("program_healthcore/risk.html", records=records)', 'def risk_dashboard():\n    from app.models.healthcore import HcRiskAssessment\n    records = HcRiskAssessment.query.filter_by(user_id=current_user.id).order_by(HcRiskAssessment.calculated_date.desc(), HcRiskAssessment.created_at.desc()).all()\n    return render_template("program_healthcore/risk.html", records=records)\n' + risk_route)

open('app/program_healthcore/routes.py', 'w').write(content)
