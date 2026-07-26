from datetime import datetime
import os
import time
import json
import google.generativeai as genai
from flask import current_app

def extract_health_document(filepath):
    """
    Extracts structured data from a health document using Gemini.
    Returns a dict containing 'document_type' and the specific structured data.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"error": "GEMINI_API_KEY not configured."}
        
    try:
        genai.configure(api_key=api_key)
        
        # Upload the file to Gemini
        uploaded_file = genai.upload_file(path=filepath)
        
        # Wait for processing (important for PDFs)
        while uploaded_file.state.name == "PROCESSING":
            time.sleep(2)
            uploaded_file = genai.get_file(uploaded_file.name)
            
        if uploaded_file.state.name == "FAILED":
            return {"error": "Document processing failed in Gemini API."}

        # Initialize the model
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        prompt = """
        You are an expert AI Health Document Extractor. Your job is to extract structured medical information from the provided health document.
        Do NOT diagnose, interpret, or hallucinate. Return ONLY raw structured JSON data based on what is exactly written in the document.

        First, identify the document type. The allowed document types are:
        - "laboratory" (for blood tests, urine tests, pathology, etc.)
        - "medication" (for prescriptions, medication lists, discharge pharmacy)
        - "imaging" (for MRI, CT Scan, X-Ray, Ultrasound reports)
        - "timeline" (for surgery reports, hospital discharge summaries, diagnosis letters)
        - "other" (if it doesn't fit the above)

        Then, extract the relevant fields for that document type.

        If it's "laboratory", output exactly in this JSON format:
        {
          "document_type": "laboratory",
          "laboratory": "Name of Lab (if present)",
          "report_date": "YYYY-MM-DD",
          "tests": [
            {
              "name": "Test Name",
              "value": "Numeric value or result",
              "unit": "Unit (e.g. mg/dL)",
              "reference_range": "Range (if present)",
              "status": "High/Low/Normal (if indicated)"
            }
          ]
        }

        If it's "medication", output exactly in this JSON format:
        {
          "document_type": "medication",
          "report_date": "YYYY-MM-DD",
          "medications": [
            {
              "name": "Medication Name",
              "dose": "Dosage (e.g. 500 mg)",
              "frequency": "Frequency (e.g. Twice daily)",
              "status": "Active or Discontinued (if known)"
            }
          ]
        }
        
        If it's "imaging", output exactly in this JSON format:
        {
          "document_type": "imaging",
          "report_date": "YYYY-MM-DD",
          "modality": "e.g. MRI, CT Scan",
          "body_part": "e.g. Brain, Chest",
          "findings": "Summary of findings",
          "impression": "Radiologist impression"
        }

        Return ONLY the JSON string. Do not include markdown code blocks like ```json ... ```. 
        """

        response = model.generate_content([uploaded_file, prompt])
        
        # Clean up the file from Gemini
        try:
            genai.delete_file(uploaded_file.name)
        except:
            pass

        # Parse JSON
        response_text = response.text.strip()
        if response_text.startswith("```json"):
            response_text = response_text.replace("```json", "").replace("```", "").strip()
        elif response_text.startswith("```"):
            response_text = response_text.replace("```", "").strip()
            
        try:
            structured_data = json.loads(response_text)
            return structured_data
        except json.JSONDecodeError:
            return {"error": "Failed to parse JSON from AI response.", "raw_response": response_text}

    except Exception as e:
        print(f"Gemini Extraction Error: {str(e)}")
        return {"error": str(e)}

def generate_risk_assessment(user_id):
    from app.models.healthcore import HcPatientProfile, HcLaboratory, HcLifestyle, HcMedication, HcTimelineEvent, HcRiskAssessment
    from app import db
    
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"error": "GEMINI_API_KEY not configured."}
        
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Gather data
        profile = HcPatientProfile.query.filter_by(user_id=user_id).first()
        labs = HcLaboratory.query.filter_by(user_id=user_id).order_by(HcLaboratory.report_date.desc()).limit(20).all()
        lifestyles = HcLifestyle.query.filter_by(user_id=user_id).order_by(HcLifestyle.log_date.desc()).limit(20).all()
        meds = HcMedication.query.filter_by(user_id=user_id, status='Active').all()
        events = HcTimelineEvent.query.filter_by(user_id=user_id).order_by(HcTimelineEvent.start_date.desc()).limit(10).all()
        
        prompt = f"""
        You are an expert AI Health Risk Assessor.
        Analyze the following patient data and generate a JSON response with risk scores and recommendations.
        
        Patient Profile:
        Age/DOB: {profile.dob if profile else 'N/A'}
        Sex: {profile.biological_sex if profile else 'N/A'}
        Weight (kg): {profile.weight_kg if profile else 'N/A'}
        Height (cm): {profile.height_cm if profile else 'N/A'}
        Chronic Conditions: {profile.chronic_conditions if profile else 'None'}
        
        Recent Labs: {[f"{l.test_name}: {l.value} {l.units}" for l in labs]}
        Recent Lifestyle logs: {[f"{l.category} - {l.metric_name}: {l.value_num or l.value_str}" for l in lifestyles]}
        Active Medications: {[m.medication_name for m in meds]}
        Medical History Events: {[e.title for e in events]}
        
        Analyze this data and return ONLY a JSON response in the following exact format:
        {{
          "risk_category": "e.g., Cardiovascular, Metabolic, Overall",
          "risk_score_100": 75,
          "risk_level": "High, Medium, or Low",
          "factors": "List of key contributing factors",
          "recommendations": "Actionable recommendations for the patient"
        }}
        
        Return ONLY the JSON string. Do not include markdown code blocks.
        """
        
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        if response_text.startswith("```json"):
            response_text = response_text.replace("```json", "").replace("```", "").strip()
        elif response_text.startswith("```"):
            response_text = response_text.replace("```", "").strip()
            
        data = json.loads(response_text)
        
        assessment = HcRiskAssessment(
            user_id=user_id,
            calculated_date=datetime.utcnow().date(),
            risk_category=data.get("risk_category", "Overall"),
            risk_score_100=data.get("risk_score_100", 0),
            risk_level=data.get("risk_level", "Unknown"),
            factors=data.get("factors", ""),
            recommendations=data.get("recommendations", "")
        )
        db.session.add(assessment)
        db.session.commit()
        return {"success": True}
        
    except Exception as e:
        print(f"Risk Gen Error: {e}")
        return {"error": str(e)}

def generate_correlation_insight(user_id):
    from app.models.healthcore import HcPatientProfile, HcLaboratory, HcLifestyle, HcCorrelationInsight
    from app import db
    
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"error": "GEMINI_API_KEY not configured."}
        
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        labs = HcLaboratory.query.filter_by(user_id=user_id).order_by(HcLaboratory.report_date.desc()).limit(30).all()
        lifestyles = HcLifestyle.query.filter_by(user_id=user_id).order_by(HcLifestyle.log_date.desc()).limit(50).all()
        
        prompt = f"""
        You are an expert AI Health Data Analyst.
        Find interesting correlations between the patient's lifestyle logs (sleep, exercise, diet, vitals) and their laboratory results over time.
        
        Recent Labs: {[f"{l.report_date}: {l.test_name}: {l.value}" for l in labs]}
        Recent Lifestyle logs: {[f"{l.log_date}: {l.category} - {l.metric_name}: {l.value_num or l.value_str}" for l in lifestyles]}
        
        Return ONLY a JSON response in the following exact format:
        {{
          "variable_a": "e.g., Sleep Duration",
          "variable_b": "e.g., Blood Pressure",
          "correlation_strength": "Strong Positive, Weak Negative, etc.",
          "description": "A clear explanation of how these two variables seem to be interacting based on the data.",
          "confidence_score": 85
        }}
        
        Return ONLY the JSON string. Do not include markdown code blocks.
        """
        
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        if response_text.startswith("```json"):
            response_text = response_text.replace("```json", "").replace("```", "").strip()
        elif response_text.startswith("```"):
            response_text = response_text.replace("```", "").strip()
            
        data = json.loads(response_text)
        
        insight = HcCorrelationInsight(
            user_id=user_id,
            generated_date=datetime.utcnow().date(),
            variable_a=data.get("variable_a", "Unknown"),
            variable_b=data.get("variable_b", "Unknown"),
            correlation_strength=data.get("correlation_strength", "Unknown"),
            description=data.get("description", ""),
            confidence_score=data.get("confidence_score", 0.0)
        )
        db.session.add(insight)
        db.session.commit()
        return {"success": True}
        
    except Exception as e:
        print(f"Correlation Gen Error: {e}")
        return {"error": str(e)}

def generate_report(user_id, report_type, audience):
    from app.models.healthcore import HcPatientProfile, HcLaboratory, HcLifestyle, HcMedication, HcTimelineEvent, HcGeneratedReport
    from app import db
    
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"error": "GEMINI_API_KEY not configured."}
        
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Gather data
        profile = HcPatientProfile.query.filter_by(user_id=user_id).first()
        labs = HcLaboratory.query.filter_by(user_id=user_id).order_by(HcLaboratory.report_date.desc()).limit(20).all()
        lifestyles = HcLifestyle.query.filter_by(user_id=user_id).order_by(HcLifestyle.log_date.desc()).limit(20).all()
        meds = HcMedication.query.filter_by(user_id=user_id, status='Active').all()
        events = HcTimelineEvent.query.filter_by(user_id=user_id).order_by(HcTimelineEvent.start_date.desc()).limit(10).all()
        
        prompt = f"""
        You are an expert Medical AI Assistant.
        Generate a comprehensive health report based on the following patient data.
        Report Type requested: {report_type}
        Target Audience: {audience}
        
        Patient Profile:
        Age/DOB: {profile.dob if profile else 'N/A'}
        Sex: {profile.biological_sex if profile else 'N/A'}
        Weight (kg): {profile.weight_kg if profile else 'N/A'}
        Height (cm): {profile.height_cm if profile else 'N/A'}
        Chronic Conditions: {profile.chronic_conditions if profile else 'None'}
        
        Recent Labs: {[f"{l.test_name}: {l.value} {l.units}" for l in labs]}
        Recent Lifestyle logs: {[f"{l.category} - {l.metric_name}: {l.value_num or l.value_str}" for l in lifestyles]}
        Active Medications: {[m.medication_name for m in meds]}
        Medical History Events: {[e.title for e in events]}
        
        Please format the report professionally using Markdown. Include sections like:
        - Executive Summary
        - Key Findings
        - Recommendations/Action Plan
        Adjust the tone and technical depth appropriately for a {audience}.
        """
        
        response = model.generate_content(prompt)
        report_summary = response.text.strip()
        
        record = HcGeneratedReport(
            user_id=user_id,
            generated_date=datetime.utcnow(),
            report_type=report_type,
            report_summary=report_summary
        )
        db.session.add(record)
        db.session.commit()
        return {"success": True}
        
    except Exception as e:
        print(f"Report Gen Error: {e}")
        return {"error": str(e)}
